package crypto

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// initTestKey 直接注入一把已知的 activeKey，跳过 Init/loadKey。
//
// 为什么不走 Init + 临时 key 文件：2026-09 起主密钥只认环境变量、不再有
// ~/.apiGateway.key 兜底，写文件那条路径已不存在。测试要的是"一把确定的
// key"，不是"loadKey 的行为"—— 后者由 TestLoadKey* 单独覆盖。
func initTestKey(t *testing.T) {
	t.Helper()
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i + 1)
	}
	mu.Lock()
	activeKey = key
	mu.Unlock()
}

func TestEncryptDecrypt(t *testing.T) {
	initTestKey(t)

	cases := []string{
		"sk-abc123",
		"Bearer some-long-token-value",
		"a",
		"with spaces and unicode: 你好",
	}
	for _, tc := range cases {
		enc, err := Encrypt(tc)
		if err != nil {
			t.Fatalf("Encrypt(%q): %v", tc, err)
		}
		if enc == tc {
			t.Errorf("Encrypt(%q) returned plaintext unchanged", tc)
		}
		if len(enc) < 4 || enc[:4] != "enc:" {
			t.Errorf("Encrypt(%q) missing enc: prefix: %q", tc, enc)
		}

		got, err := Decrypt(enc)
		if err != nil {
			t.Fatalf("Decrypt(%q): %v", enc, err)
		}
		if got != tc {
			t.Errorf("roundtrip: got %q, want %q", got, tc)
		}
	}
}

func TestEncryptEmpty(t *testing.T) {
	initTestKey(t)
	enc, err := Encrypt("")
	if err != nil {
		t.Fatal(err)
	}
	if enc != "" {
		t.Errorf("Encrypt(\"\") should return \"\", got %q", enc)
	}
	dec, err := Decrypt("")
	if err != nil {
		t.Fatal(err)
	}
	if dec != "" {
		t.Errorf("Decrypt(\"\") should return \"\", got %q", dec)
	}
}

func TestDecryptLegacyPlaintext(t *testing.T) {
	initTestKey(t)
	// A value without the "enc:" prefix is returned as-is (migration path).
	plain := "sk-legacy-token"
	got, err := Decrypt(plain)
	if err != nil {
		t.Fatal(err)
	}
	if got != plain {
		t.Errorf("Decrypt(legacy) = %q, want %q", got, plain)
	}
}

func TestEncryptNonDeterministic(t *testing.T) {
	initTestKey(t)
	a, _ := Encrypt("same-value")
	b, _ := Encrypt("same-value")
	if a == b {
		t.Error("two Encrypt calls on same plaintext produced identical ciphertext (nonce not random?)")
	}
}

// TestLoadKeyFromEnv 是新契约的主路径：只认环境变量。
func TestLoadKeyFromEnv(t *testing.T) {
	hexKey := strings.Repeat("ab", 32) // 64 hex chars = 32 bytes
	t.Setenv(envKeyName, hexKey)

	key, source, err := loadKey()
	if err != nil {
		t.Fatalf("loadKey: %v", err)
	}
	if len(key) != 32 {
		t.Errorf("key length = %d, want 32", len(key))
	}
	if source != "env:"+envKeyName {
		t.Errorf("source = %q, want %q", source, "env:"+envKeyName)
	}

	// 首尾空白容忍：从 .env / shell 复制常带换行或引号。
	t.Setenv(envKeyName, "  "+hexKey+"\n")
	if _, _, err := loadKey(); err != nil {
		t.Errorf("loadKey with surrounding whitespace: %v", err)
	}
}

// TestLoadKeyMissingIsFatal 锁住本次改动的**核心意图**：缺 key 必须报错，
// 绝不静默生成。这是防止"容器重建 → 新随机 key → 历史密文全废"的关键。
func TestLoadKeyMissingIsFatal(t *testing.T) {
	t.Setenv(envKeyName, "")

	_, _, err := loadKey()
	if err == nil {
		t.Fatal("loadKey with empty env = nil error, want error (must not auto-generate)")
	}
	if !errors.Is(err, errMissingEnv) {
		t.Errorf("error is not errMissingEnv: %v", err)
	}
	// 错误信息必须可操作：包含环境变量名与生成命令。
	msg := err.Error()
	for _, want := range []string{envKeyName, "openssl rand -hex 32"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error message missing %q: %s", want, msg)
		}
	}
}

// TestLoadKeyRejectsMalformed：格式错误必须在启动时暴露，不能带着坏 key 跑。
func TestLoadKeyRejectsMalformed(t *testing.T) {
	cases := []struct{ name, val string }{
		{"not hex", strings.Repeat("zz", 32)},
		{"too short", strings.Repeat("ab", 8)},
		{"too long", strings.Repeat("ab", 33)},
		{"odd length", strings.Repeat("a", 63)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Setenv(envKeyName, c.val)
			if _, _, err := loadKey(); err == nil {
				t.Errorf("loadKey(%q) = nil error, want error", c.val)
			}
		})
	}
}

// TestLoadKeyIgnoresLegacyFile 是本次改动的回归护栏：即使 home 目录里躺着
// 旧版密钥文件，也**绝不能**拿它当主密钥（那等于 B 方案没落地）。
func TestLoadKeyIgnoresLegacyFile(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)
	t.Setenv("USERPROFILE", dir) // Windows
	legacy := filepath.Join(dir, legacyKeyFile)
	if err := os.WriteFile(legacy, []byte(strings.Repeat("cd", 32)), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(envKeyName, "")

	_, _, err := loadKey()
	if err == nil {
		t.Fatal("loadKey fell back to the legacy key file; it must be env-only")
	}

	// 但错误信息里应带上迁移指引（帮旧部署用户一步完成迁移）。
	if p := legacyKeyFilePath(); p != "" {
		if !strings.Contains(legacyKeyHint(), legacyKeyFile) {
			t.Error("legacyKeyHint should mention the legacy file name")
		}
	}
}

// TestLegacyKeyHintEmptyWhenNoFile：没有遗留文件时提示为空串，不影响主流程。
func TestLegacyKeyHintEmptyWhenNoFile(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)
	t.Setenv("USERPROFILE", dir)
	if got := legacyKeyHint(); got != "" {
		t.Errorf("legacyKeyHint with no legacy file = %q, want empty", got)
	}
}

// TestLegacyKeyHintIgnoresGarbageFile：遗留文件内容不是合法 key 时不该给出
// 误导性的迁移命令（否则用户会导出一把垃圾）。
func TestLegacyKeyHintIgnoresGarbageFile(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)
	t.Setenv("USERPROFILE", dir)
	if err := os.WriteFile(filepath.Join(dir, legacyKeyFile), []byte("not-a-key"), 0600); err != nil {
		t.Fatal(err)
	}
	if got := legacyKeyHint(); got != "" {
		t.Errorf("legacyKeyHint with garbage file = %q, want empty", got)
	}
}

func hexOf(b []byte) string {
	const hx = "0123456789abcdef"
	out := make([]byte, len(b)*2)
	for i, v := range b {
		out[i*2] = hx[v>>4]
		out[i*2+1] = hx[v&0xf]
	}
	return string(out)
}

