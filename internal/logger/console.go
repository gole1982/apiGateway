package logger

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
)

// This file implements the structured console logger for the whole process.
//
// All operator-facing process logs (startup, routing summary, upstream errors,
// DB migration notices, scheduler probes, ...) used to be scattered as free-form
// `log.Printf("[TAG] msg=%s ...")` calls across 7 packages with no shared schema,
// hand-aligned spacing, and per-call ad-hoc key/value formatting. That made the
// output unparseable by log collectors (Loki/ELK) and made adding a cross-cutting
// field (e.g. request_id, component) require touching every call site.
//
// ConsoleLogger wraps a single *slog.Logger backed by a JSON Lines handler so
// every record shares one schema:
//
//   {"time":"2026-08-02T12:00:00.123Z","level":"info","component":"gateway",
//    "msg":"upstream ok","request_id":"req-...","rapi":"gpt-4","status":200,
//    "latency_ms":1234}
//
// The original `[TAG]` prefixes are preserved as part of the message so operators
// can still grep for them, and they double as the level-mapping key via
// tagLevel() (e.g. [ERR]/[FAIL]→error, [WARN]→warn, everything else→info).
//
// Output goes to os.Stderr by default (same destination as the old stdlib `log`
// default); enabling ConsoleOptions.FilePath also tees the same stream to a
// JSON file (e.g. logs/gateway.log) without requiring any third-party rotation
// library.

// ConsoleOptions configures the process-wide ConsoleLogger.
type ConsoleOptions struct {
	// Level sets the minimum verbosity. Defaults to slog.LevelInfo when nil.
	Level slog.Leveler
	// FilePath, when non-empty AND EnableFile is true, additionally tees the
	// JSON stream to this file (created/append, parent dirs auto-created).
	FilePath string
	// EnableFile toggles the file tee. When false FilePath is ignored.
	EnableFile bool
}

// ConsoleLogger is a thin wrapper over *slog.Logger that bakes in a "component"
// attribute (which package emitted the log) and exposes convenience helpers
// matching the old call sites' shape (Info/Warn/Error with key/value pairs).
type ConsoleLogger struct {
	sl     *slog.Logger
	component string
}

var (
	consoleOnce    sync.Once
	defaultConsole atomic.Pointer[ConsoleLogger]
)

// InitConsoleLogger initialises (or re-initialises) the process-wide
// ConsoleLogger. It is safe to call more than once; the first call wins and
// subsequent calls are ignored, so package-level init order does not matter.
// Returns the resulting ConsoleLogger for convenience.
//
// As a side effect, it installs the same underlying handler as the stdlib
// slog default (slog.SetDefault) so packages that cannot import this package
// (e.g. db, which the logger package itself depends on and would form an
// import cycle) can emit records through the plain slog API and still land on
// the same JSON stream with the same configuration.
func InitConsoleLogger(opts ConsoleOptions) *ConsoleLogger {
	c := buildConsoleLogger(opts)
	// Store unconditionally on the first call so a mis-timed second init cannot
	// replace a logger that callers have already cached via DefaultConsole().
	// On later calls we keep the existing one to avoid log routing surprises.
	if existing := defaultConsole.Load(); existing != nil {
		return existing
	}
	defaultConsole.Store(c)
	// Mirror the handler into the stdlib slog default. Packages below us in the
	// import graph use slog.Info/Error/etc. which dispatch through slog.Default();
	// by pointing it at the same JSON handler we keep a single, consistent
	// output stream and avoid duplicate / divergent formatting.
	slog.SetDefault(c.sl)
	return c
}

// DefaultConsole returns the process-wide ConsoleLogger, initialising it with
// stderr-only defaults if InitConsoleLogger was never called. This guarantees
// that early callers (e.g. before service.New finishes wiring config) still
// get a working structured logger instead of the raw stdlib `log` default.
func DefaultConsole() *ConsoleLogger {
	if c := defaultConsole.Load(); c != nil {
		return c
	}
	// Single-flight: avoid two goroutines creating distinct default loggers.
	consoleOnce.Do(func() {
		c := buildConsoleLogger(ConsoleOptions{Level: slog.LevelInfo})
		defaultConsole.Store(c)
	})
	return defaultConsole.Load()
}

func buildConsoleLogger(opts ConsoleOptions) *ConsoleLogger {
	level := opts.Level
	if level == nil {
		level = slog.LevelInfo
	}

	// Default sink is stderr, mirroring the stdlib `log` default so operators
	// who already collect stderr see no routing change.
	var w io.Writer = os.Stderr
	if opts.EnableFile && opts.FilePath != "" {
		if file, err := openLogFile(opts.FilePath); err == nil {
			w = io.MultiWriter(os.Stderr, file)
			// MultiWriter keeps both sinks in sync on every Write; since the
			// JSON handler writes one record per Write, both sinks get the
			// same newline-delimited JSON. The file is append-only, never
			// closed during the process; rotation is an operator concern.
		} else {
			// Fall back to stderr-only; surface the failure via the logger
			// itself once it is built (below) rather than panic at startup.
			defer func() {
				// Logging after the handler exists is preferable to a fatal
				// exit: a broken optional file sink must not take the gateway
				// down.
				buildConsoleLogger(ConsoleOptions{Level: level}).
					Error("logger", "log file open failed; defaulting to stderr",
						"path", opts.FilePath, "error", err.Error())
			}()
		}
	}

	handler := slog.NewJSONHandler(w, &slog.HandlerOptions{
		Level: level,
		// ReplaceAttr: keep the default time key ("time") — operators expect
		// the slog default. No replacement here keeps timestamps RFC3339Nano.
	})
	sl := slog.New(handler)
	return &ConsoleLogger{sl: sl, component: ""}
}

func openLogFile(path string) (*os.File, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	return os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
}

// With returns a child ConsoleLogger that adds persistent attributes to every
// record it emits. This is how the request lifetime attaches request_id once
// at the top of ServeHTTP and has it flow through every downstream log call:
//
//	reqLog := logger.DefaultConsole().With("request_id", requestID)
//	reqLog.Info("gateway", "upstream ok", "rapi", alias, "status", 200)
//
// With accepts an even number of key/value pairs (chained .With calls are
// additive), so a single call can attach several fields at once:
//
//	reqLog.With("rapi", alias, "model", model, "attempt", n)
func (c *ConsoleLogger) With(kv ...any) *ConsoleLogger {
	if c == nil {
		return nil
	}
	if len(kv)%2 != 0 {
		// Odd arity would corrupt pairing; drop the trailing value to keep the
		// logger usable and surface the bug via the missing-value marker.
		kv = append(append([]any{}, kv...), "<missing value>")
	}
	child := c.sl
	for i := 0; i+1 < len(kv); i += 2 {
		key, ok := kv[i].(string)
		if !ok {
			key = "<invalid key>"
		}
		child = child.With(slog.Any(key, kv[i+1]))
	}
	return &ConsoleLogger{sl: child, component: c.component}
}

// WithComponent returns a child whose records are tagged with the given
// component (the emitting package, e.g. "gateway", "service", "db"). The
// component is hoisted into a top-level attribute so dashboards can filter by
// "component=gateway" without parsing the message.
func (c *ConsoleLogger) WithComponent(component string) *ConsoleLogger {
	if c == nil {
		return nil
	}
	return &ConsoleLogger{
		sl:        c.sl.With(slog.String("component", component)),
		component: component,
	}
}

// Info/Warn/Error/Debug emit a record at the given level. The component
// argument is the emitting package name (callers pass a literal such as
// "gateway"); msg is a short event description; key/value pairs (len must be
// even) become structured fields. Odd pair counts log a separate error so the
// original record is not silently dropped.
func (c *ConsoleLogger) Info(component, msg string, kv ...any) {
	c.emit(slog.LevelInfo, component, msg, kv)
}
func (c *ConsoleLogger) Warn(component, msg string, kv ...any) {
	c.emit(slog.LevelWarn, component, msg, kv)
}
func (c *ConsoleLogger) Error(component, msg string, kv ...any) {
	c.emit(slog.LevelError, component, msg, kv)
}
func (c *ConsoleLogger) Debug(component, msg string, kv ...any) {
	c.emit(slog.LevelDebug, component, msg, kv)
}

func (c *ConsoleLogger) emit(level slog.Level, component, msg string, kv []any) {
	if c == nil {
		// A nil ConsoleLogger should never happen in production (DefaultConsole
		// lazily allocates), but guard anyway so a misordered init cannot nil-
		// deref the gateway during shutdown.
		return
	}
	// Build attrs from the caller's key/value pairs first. If kv is unbalanced,
	// append a placeholder error value so the original record is not silently
	// dropped — a missing key at a call site is a bug we want to see, and a
	// panic here would kill the gateway for a logging typo.
	pairs := kv
	if len(pairs)%2 != 0 {
		pairs = append(append([]any{}, pairs...), "<missing value>")
	}
	attrs := toAttrs(pairs)
	// Prepend component as a first-class attribute. Doing this AFTER toAttrs
	// (rather than stuffing a slog.Attr into the flat kv slice) avoids the
	// pairing bug where an Attr would be mistaken for a string key and shift
	// every subsequent pair: component must be a real Attr, not a kv entry.
	attrs = append([]slog.Attr{slog.String("component", component)}, attrs...)
	c.sl.LogAttrs(nil, level, msg, attrs...)
}

// toAttrs converts a flat key/value slice into []slog.Attr, treating every
// other element as an attribute name. Elements already of type slog.Attr are
// appended verbatim and do NOT consume the following element, so callers can
// intermix raw key/value pairs with pre-built Attr objects. Non-string keys
// (other than slog.Attr) are replaced with a visible "<invalid key>" marker
// rather than silently dropping the pair.
func toAttrs(kv []any) []slog.Attr {
	out := make([]slog.Attr, 0, len(kv)/2)
	i := 0
	for i < len(kv) {
		// A pre-built Attr slot passes through unchanged and does not consume
		// the next element. This is why callers can safely prepend
		// slog.String("component", ...) to the flat slice if they choose.
		if attr, ok := kv[i].(slog.Attr); ok {
			out = append(out, attr)
			i++
			continue
		}
		if i+1 >= len(kv) {
			// Trailing key with no value: emit it with a visible placeholder so
			// the imbalance is obvious in the output instead of being silently
			// truncated.
			out = append(out, slog.Any("<invalid key>", kv[i]))
			break
		}
		key, ok := kv[i].(string)
		if !ok {
			key = "<invalid key>"
		}
		out = append(out, slog.Any(key, kv[i+1]))
		i += 2
	}
	return out
}

// LogByTag emits a record using the legacy `[TAG] ...` message convention.
// The tag drives the log level via tagLevel(); the original "[TAG] " prefix is
// preserved in the message so existing grep-based alerts keep working while the
// rest of the record is structured. This is the lowest-friction migration path
// for the ~100 existing log.Printf call sites: drop the format string into
// structured key/value pairs and call LogByTag with the same tag.
func (c *ConsoleLogger) LogByTag(tag, component, msg string, kv ...any) {
	level := tagLevel(tag)
	// Keep the tag at the front of the message so `grep "\[ERR\]"` still hits.
	// The leading space normalisation keeps the old alignment-readable feel for
	// human tail users without breaking the JSON structure.
	full := tag + " " + msg
	c.emit(level, component, full, kv)
}

// tagLevel maps a legacy bracketed tag (without the brackets) to a slog level.
// Unknown tags default to info — the old tags were predominantly informational,
// and misclassifying a rare one as info is far less harmful than burying a real
// error under debug-level filtering.
func tagLevel(tag string) slog.Level {
	// Strip surrounding brackets if the caller passed the raw tag with them.
	t := strings.TrimSpace(tag)
	t = strings.TrimPrefix(t, "[")
	t = strings.TrimSuffix(t, "]")
	switch strings.ToUpper(t) {
	case "WARN":
		return slog.LevelWarn
	case "ERR", "FAIL", "KEY-FAIL", "FATAL":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// SL returns the underlying *slog.Logger for callers that want to use slog
// primitives directly (e.g. slog.Group). Prefer the convenience methods above
// for normal logs so component tagging stays consistent.
func (c *ConsoleLogger) SL() *slog.Logger {
	if c == nil {
		return nil
	}
	return c.sl
}

// ParseLevel maps a human-friendly level string ("debug"/"info"/"warn"/"error",
// case-insensitive) to a slog.Level. Unknown values default to info, matching
// the historical behaviour where every console log was effectively info-level
// under the stdlib `log` default.
func ParseLevel(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error", "err":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
