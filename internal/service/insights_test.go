package service

import (
	"testing"
	"time"

	"gateway/internal/db"
	"gateway/internal/models"
	"gateway/internal/scheduler"
)

func TestBuildHealthEnrichesPlatformContext(t *testing.T) {
	now := time.Now()
	snap := scheduler.Snapshot{
		RAPIs: []scheduler.RAPISnapshot{
			{ID: 101, Cooling: true, Reason: "rate limited", RecoverAt: now.Add(time.Minute), ConsecutiveFailures: 3},
		},
		Keys: []scheduler.KeySnapshot{
			{ID: 501, Cooling: true, Reason: "401 unauthorized", RecoverAt: now.Add(2 * time.Minute)},
		},
	}
	rapiCfgByID := map[int64]rapiCfg{
		101: {Alias: "glm-5.2", PlatformID: 7, KeyIDs: "501"},
	}
	platformByID := map[int64]models.Platform{
		7: {ID: 7, Name: "京东", BillingAddress: "https://billing.jd.com"},
	}
	keyByID := map[int64]models.PlatformKey{
		501: {ID: 501, PlatformID: 7, KeyIndex: 0, Label: "主Key"},
	}

	h := buildHealth(snap, map[int64]db.RAPIStat{}, rapiCfgByID, platformByID, keyByID)

	if len(h.CoolingRAPIs) != 1 {
		t.Fatalf("CoolingRAPIs len = %d, want 1", len(h.CoolingRAPIs))
	}
	cr := h.CoolingRAPIs[0]
	if cr.PlatformID != 7 || cr.PlatformName != "京东" || cr.BillingAddress != "https://billing.jd.com" {
		t.Errorf("CoolingRAPI platform context = %+v", cr)
	}
	if cr.KeyIDs != "501" {
		t.Errorf("CoolingRAPI KeyIDs = %q, want \"501\"", cr.KeyIDs)
	}

	if len(h.CoolingKeys) != 1 {
		t.Fatalf("CoolingKeys len = %d, want 1", len(h.CoolingKeys))
	}
	ck := h.CoolingKeys[0]
	if ck.PlatformID != 7 || ck.PlatformName != "京东" || ck.Label != "主Key" || ck.KeyIndex != 0 {
		t.Errorf("CoolingKey platform context = %+v", ck)
	}
	if ck.BillingAddress != "https://billing.jd.com" {
		t.Errorf("CoolingKey BillingAddress = %q", ck.BillingAddress)
	}
}

func TestBuildHealthEmptyWhitelistAndUnknownPlatform(t *testing.T) {
	now := time.Now()
	snap := scheduler.Snapshot{
		RAPIs: []scheduler.RAPISnapshot{
			// Empty key_ids whitelist (uses all platform keys), platform unknown to lookup.
			{ID: 202, Cooling: true, Reason: "timeout", RecoverAt: now.Add(time.Minute), ConsecutiveFailures: 1},
		},
		Keys: []scheduler.KeySnapshot{},
	}
	rapiCfgByID := map[int64]rapiCfg{
		202: {Alias: "orphan-model", PlatformID: 999, KeyIDs: ""},
	}
	h := buildHealth(snap, map[int64]db.RAPIStat{}, rapiCfgByID, map[int64]models.Platform{}, map[int64]models.PlatformKey{})

	if len(h.CoolingRAPIs) != 1 {
		t.Fatalf("CoolingRAPIs len = %d, want 1", len(h.CoolingRAPIs))
	}
	cr := h.CoolingRAPIs[0]
	if cr.PlatformID != 999 || cr.PlatformName != "" || cr.BillingAddress != "" {
		t.Errorf("unknown platform should yield empty name/billing: %+v", cr)
	}
	if len(h.CoolingKeys) != 0 {
		t.Errorf("CoolingKeys len = %d, want 0", len(h.CoolingKeys))
	}
}
