// Package entity owns the finite state machines for gateway entities (RAPI,
// PlatformKey). Each entity embeds an fsm.Machine; all state transitions flow
// through the machine's transition table, and persistence side-effects are
// attached to transitions via a Store interface.
//
// This centralizes the state maintenance that previously lived in three
// places — DB columns (available / failure_type / enabled), the scheduler's
// in-memory structs (unavailableUntil / invalidated / consecutiveFailures),
// and the gateway's ad-hoc orchestration (classifyFailure / allKeysHardDead).
// Now the entity is the single source of truth: the gateway fires events, the
// entity transitions states and persists the result.
package entity

import (
	"strings"
	"time"

	"gateway/internal/fsm"
	"gateway/internal/models"
)

// State aliases the FSM state type for convenience.
type State = fsm.State

// Event aliases the FSM event type for convenience.
type Event = fsm.Event

// Well-known entity states.
const (
	StateHealthy         State = "healthy"
	StateCooling         State = "cooling"         // session-scope cooldown; auto-recovers via timer
	StatePlatformFailed  State = "platform_failed" // platform-scope cooldown (quota/overload); pinned, auto-recovers
	StateInvalidated     State = "invalidated"     // persisted unavailable / admin disabled; only Revalidate leaves
	StatePermanentFailed State = "permanent_failed" // key failure_type=2; only Success/Enable leaves
	StateDisabled        State = "disabled"         // key enabled=0 in DB
	StateExpired         State = "expired"          // key past ExpiresAt
	StateUnavailable     State = "unavailable"      // platform unavailable (available=false in DB)
)

// Well-known entity events.
const (
	EventSuccess          Event = "success"
	EventSessionFailure   Event = "session_failure"
	EventPlatformFailure  Event = "platform_failure"
	EventPermanentFailure Event = "permanent_failure"
	EventInvalidate       Event = "invalidate"
	EventRevalidate       Event = "revalidate"
	EventAllKeysSoft      Event = "all_keys_soft" // all keys cooling — transient, short cooldown
	EventAllKeysHard      Event = "all_keys_hard" // all keys dead — persist unavailable + invalidate
	EventDisable          Event = "disable"
	EventEnable           Event = "enable"
	EventExpire           Event = "expire"
	EventDetectSuccess    Event = "detect_success"
	EventDetectFailure    Event = "detect_failure"
)

// FailureScope classifies the scope of an upstream HTTP error.
type FailureScope int

const (
	// ScopeSession is a request-level or transient infra failure: auto-recovers.
	ScopeSession FailureScope = iota
	// ScopeSystem is a token/auth failure (401) fixable by refreshing the key.
	ScopeSystem
	// ScopePlatform is an account-level problem (402/403/409/423/451) needing admin action.
	ScopePlatform
)

// ClassifyFailure maps an upstream HTTP status code to a failure scope.
//
// System (401): token invalid — key marked permanently failed.
// Platform (402/403/409/423/451): account-level — key marked permanently failed.
// Session (everything else): request-level or transient infra — short cooldown.
func ClassifyFailure(statusCode int) FailureScope {
	switch statusCode {
	case 401: // Unauthorized
		return ScopeSystem
	case 402, 403, 409, 423, 451: // Payment Required, Forbidden, Conflict, Locked, Unavailable For Legal Reasons
		return ScopePlatform
	default:
		return ScopeSession
	}
}

// recoverableBillingKeywords match account-level errors that are recoverable
// without operator action once the account is topped up: quota/billing errors
// like JD's code 1058 "用户积分不足" (insufficient credits). These were the
// 8-2 incident: a 403 marked the key permanently failed even though recharging
// restored service. Matching on the error body downgrades them from permanent
// (failure_type=2) to temporary (failure_type=1) so the key auto-recovers.
var recoverableBillingKeywords = []string{
	"积分不足", // JD code 1058 用户积分不足
	"余额不足",
	"欠费",
	"余额为0",
	"余额为零",
	"insufficient balance",
	"insufficient_balance",
	"insufficient quota",
	"insufficient_quota",
	"billing",
	"payment required",
	"quota exhausted",
	"exceeded your current quota",
	"account balance",
}

// IsRecoverableBillingError reports whether an account-level (402/403/409/423/451)
// response body indicates a billing/quota problem that resolves once the account
// is topped up — as opposed to a hard ban or auth failure. When true, the gateway
// treats the failure as temporary (short cooldown, auto-recovery) instead of
// marking the key permanently failed.
func IsRecoverableBillingError(body string) bool {
	if body == "" {
		return false
	}
	lower := strings.ToLower(body)
	for _, kw := range recoverableBillingKeywords {
		if strings.Contains(lower, kw) {
			return true
		}
	}
	return false
}

// capabilityMismatchKeywords match key×model capability errors: the upstream
// rejects THIS key for THIS model because the platform's key→model permission
// table changed, as opposed to account/key-level problems. Detecting them lets
// the gateway block the (key, model) pair instead of cooling the whole key — a
// key that lost access to one model must keep serving every other model.
var capabilityMismatchKeywords = []string{
	"model not found",
	"model_not_found",
	"model not exist",
	"model does not exist",
	"model doesn't exist",
	"does not exist", // catches "The model 'foo' does not exist" (name in between)
	"doesn't exist",
	"no such model",
	"unknown model",
	"invalid model",
	"invalid_model",
	"model not enabled",
	"模型不存在",
	"无此模型",
	"该模型不存在",
	"模型未开通",
	"模型不存在或已下线",
}

// IsCapabilityMismatch reports whether a non-2xx response body indicates the
// key is not permitted to access the requested model — a key×model capability
// mismatch — rather than a generic request failure. The gateway gates this on
// 400/403/404/422 responses so transient 5xx bodies never match. When true, the
// gateway records a key×model block: the pair is skipped for this model while
// the key keeps serving the models it is allowed to.
func IsCapabilityMismatch(body string) bool {
	if body == "" {
		return false
	}
	lower := strings.ToLower(body)
	for _, kw := range capabilityMismatchKeywords {
		if strings.Contains(lower, kw) {
			return true
		}
	}
	return false
}

// CooldownConfig carries the cooldown knobs shared by RAPI and Key entities.
type CooldownConfig struct {
	DefaultCooldown    time.Duration
	TimeoutCooldown    time.Duration
	MaxCooldown        time.Duration
	ExponentialBackoff bool
	// BillingCooldown is the cooldown applied to recoverable billing errors
	// (欠费/积分不足/quota exhausted). Longer than DefaultCooldown so an
	// out-of-credit key does not burn a failed upstream attempt on every
	// request; it stays out of the pool until the cooldown elapses (by which
	// time the account is usually topped up). 0 disables the distinction.
	BillingCooldown time.Duration
}

// Store is the persistence side of entity state maintenance. Effects on state
// transitions call these methods so the DB always reflects the FSM. A nil
// Store (or nil-safe implementations) is used in tests.
type Store interface {
	// SetPlatformAvailable persists platform.available.
	SetPlatformAvailable(id int64, available bool) error
	// SetRAPIUnavailable persists rapi.available (and its reason). available=true
	// clears the reason.
	SetRAPIUnavailable(id int64, available bool, reason string) error
	// MarkKeyPermanentFailure sets failure_type=2.
	MarkKeyPermanentFailure(keyID int64, reason string) error
	// MarkKeyTemporaryFailure sets failure_type=1.
	MarkKeyTemporaryFailure(keyID int64, reason string) error
	// ClearKeyFailure resets failure_type=0.
	ClearKeyFailure(keyID int64) error
}

// SessionFailurePayload carries the reason/retry information for a session
// (transient) failure event. PersistTemporary controls whether the entity also
// writes failure_type=1 (used for HTTP 429/5xx; network errors skip the write).
// IsBilling marks a recoverable billing/quota error (欠费/积分不足) so the
// entity applies the longer BillingCooldown instead of DefaultCooldown.
type SessionFailurePayload struct {
	Reason            string
	RetryAt           time.Time
	IsTimeout         bool
	IsBilling         bool
	PersistTemporary bool
}

// AllKeysPayload distinguishes the soft (all cooling) vs hard (all dead) forms
// of the RAPI-level all-keys-unavailable event.
type AllKeysPayload struct {
	Hard   bool
	Reason string
}

// keyRowUsable reports whether a DB row is usable by its persisted facts alone
// (enabled, not permanently failed, not expired). This is the authoritative
// "key is not hard-dead" predicate shared by PickAvailableKey (via the Key
// entity) and KeysHardDead — keep them aligned.
func keyRowUsable(k models.PlatformKey, now time.Time) bool {
	if !k.Enabled || k.FailureType == 2 {
		return false
	}
	if k.ExpiresAt != nil && !k.ExpiresAt.IsZero() && now.After(*k.ExpiresAt) {
		return false
	}
	return true
}

// KeysHardDead reports whether every key in the pool is unusable without
// operator intervention (disabled, permanently failed with failure_type=2, or
// past ExpiresAt) — as opposed to merely cooling down from transient
// failures, which auto-recover. Returns the first representative failure
// reason for surfacing in the dashboard unavailable_reason field. An empty
// pool is never hard-dead (tryKeyForRAPI falls back to the legacy platform
// token as a synthetic key in that case).
func KeysHardDead(keys []models.PlatformKey) (bool, string) {
	if len(keys) == 0 {
		return false, ""
	}
	now := time.Now()
	dead := 0
	var reason string
	for _, k := range keys {
		switch {
		case !k.Enabled:
			dead++
			if reason == "" {
				reason = "Key 已禁用"
			}
		case k.FailureType == 2:
			dead++
			if reason == "" {
				reason = k.FailureReason
				if reason == "" {
					reason = "Key 永久失效 (401/402/403)"
				}
			}
		case k.ExpiresAt != nil && !k.ExpiresAt.IsZero() && now.After(*k.ExpiresAt):
			dead++
			if reason == "" {
				reason = "Key 已过期"
			}
		default:
			return false, ""
		}
	}
	return true, reason
}
