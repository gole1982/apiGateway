package service

import (
	"os"
	"strings"
	"testing"

	"gateway/internal/bundle"
)

// 回归：2026-09-26 面板密钥行显示"—"事故。
//
// 三个计数源（fillSBTables / handleSyncCenter tables / localTableCounts）
// 曾经各写各的表名字面量：platform_keys 旧名、bundle 复数键 credentials/bindings。
// 中心 v2 重建删掉 platform_keys 后，面板密钥列 404 显示"—"、推送 toast 显示 0，
// 而全部单测全绿 —— service 包覆盖率仅 1.8%，接线处根本没有测试。
//
// 修法：三处同源（centerDefinitionTables）。本文件锁死三件事：
//   1. 规范表名恰好是 v2 的 6 张，不多不少，没有旧名；
//   2. 本地计数器的键集合与规范表名一致（只比键，不执行，无需 DB）；
//   3. bundle 取数把每张表映射到正确的数组；
//   4. dashboard.html 的两处 v-for、标签映射、推送 toast 用同一套表名，
//      且全文不再出现 platform_keys。

var wantDefinitionTables = []string{
	"platform", "credential", "endpoint_credential",
	"rapi", "lapi", "lapi_rapi_order",
}

func TestDefinitionTablesAreV2(t *testing.T) {
	if len(centerDefinitionTables) != len(wantDefinitionTables) {
		t.Fatalf("centerDefinitionTables = %v, want %v", centerDefinitionTables, wantDefinitionTables)
	}
	for i, want := range wantDefinitionTables {
		if centerDefinitionTables[i] != want {
			t.Fatalf("centerDefinitionTables[%d] = %q, want %q (full: %v)",
				i, centerDefinitionTables[i], want, centerDefinitionTables)
		}
	}
	for _, tbl := range centerDefinitionTables {
		if tbl == "platform_keys" {
			t.Error("centerDefinitionTables still contains the dropped v1 table platform_keys")
		}
	}
}

func TestLocalTableCounterKeysMatch(t *testing.T) {
	if len(localTableCounters) != len(centerDefinitionTables) {
		t.Fatalf("localTableCounters has %d entries, want %d (%v)",
			len(localTableCounters), len(centerDefinitionTables), centerDefinitionTables)
	}
	seen := map[string]bool{}
	for _, c := range localTableCounters {
		if seen[c.key] {
			t.Errorf("duplicate local counter key %q", c.key)
		}
		seen[c.key] = true
	}
	for _, tbl := range centerDefinitionTables {
		if !seen[tbl] {
			t.Errorf("table %q has no local counter; its 本地 column will render —", tbl)
		}
	}
}

func TestBundleTableCountMapsEachTable(t *testing.T) {
	b := &bundle.Bundle{
		Platforms:     make([]bundle.Platform, 1),
		Credentials:   make([]bundle.Credential, 2),
		Bindings:      make([]bundle.CredentialBinding, 3),
		RAPIs:         make([]bundle.RAPI, 4),
		LAPIs:         make([]bundle.LAPI, 5),
		LAPIRapiOrder: make([]bundle.LAPIRapiOrder, 6),
	}
	want := map[string]int{
		"platform": 1, "credential": 2, "endpoint_credential": 3,
		"rapi": 4, "lapi": 5, "lapi_rapi_order": 6,
	}
	for _, tbl := range centerDefinitionTables {
		if got := bundleTableCount(b, tbl); got != want[tbl] {
			t.Errorf("bundleTableCount(%q) = %d, want %d (wrong array mapped)", tbl, got, want[tbl])
		}
	}
	if got := bundleTableCount(b, "platform_keys"); got != -1 {
		t.Errorf("bundleTableCount(platform_keys) = %d, want -1 (unknown table must not masquerade as empty)", got)
	}
	if got := bundleTableCount(nil, "platform"); got != -1 {
		t.Errorf("bundleTableCount(nil) = %d, want -1", got)
	}
}

func TestDashboardUsesV2TableNames(t *testing.T) {
	raw, err := os.ReadFile("dashboard.html")
	if err != nil {
		t.Fatalf("read dashboard.html: %v", err)
	}
	src := string(raw)

	// 两处 v-for（sync 卡 + sb-config 卡）必须列出全部 6 张 v2 表。
	vfor := "'platform','credential','endpoint_credential','rapi','lapi','lapi_rapi_order'"
	if n := strings.Count(src, vfor); n != 2 {
		t.Errorf("v-for table array found %d times, want 2 (sync card + sb-config card)", n)
	}
	// 标签映射。
	for _, pair := range [][2]string{
		{"credential", "密钥"}, {"endpoint_credential", "绑定"},
	} {
		if !strings.Contains(src, pair[0]+": '"+pair[1]+"'") {
			t.Errorf("tableNameLabel missing %s: '%s'", pair[0], pair[1])
		}
	}
	// 推送 toast 必须读新键（旧键 ct.platform_keys 恒为 undefined → 显示 0）。
	if !strings.Contains(src, "ct.credential") {
		t.Error("push toast does not read ct.credential; it will render key x0")
	}
	// 全文不留旧表名。dashboard 只消费接口字段（k.key_index / rapi.key_ids），
	// 那些是本地派生列与接口形状，与已删的中心表无关。
	if strings.Contains(src, "platform_keys") {
		t.Error("dashboard.html still references platform_keys; the v1 center table is gone")
	}
}
