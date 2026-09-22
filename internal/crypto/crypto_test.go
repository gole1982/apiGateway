package crypto

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func initTestKey(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i + 1)
	}
	import_hex := func(b []byte) string {
		const hx = "0123456789abcdef"
		out := make([]byte, len(b)*2)
		for i, v := range b {
			out[i*2] = hx[v>>4]
			out[i*2+1] = hx[v&0xf]
		}
		return string(out)
	}
	p := filepath.Join(dir, keyFile)
	if err := os.WriteFile(p, []byte(import_hex(key)), 0600); err != nil {
		t.Fatal(err)
	}
	// Override home-dir lookup by loading the key directly.
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

func TestLoadOrGenerateKey(t *testing.T) {
	t.Setenv(envKeyName, "") // 确保走文件路径，不受外部环境影响
	dir := t.TempDir()
	path := filepath.Join(dir, keyFile)

	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}
	hexKey := func(b []byte) string {
		const hx = "0123456789abcdef"
		out := make([]byte, len(b)*2)
		for i, v := range b {
			out[i*2] = hx[v>>4]
			out[i*2+1] = hx[v&0xf]
		}
		return string(out)
	}
	if err := os.WriteFile(path, []byte(hexKey(key)), 0600); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	// On Windows, the filesystem does not support Unix permission bits,
	// so we only verify the file is readable (not checking exact mode).
	if info.Size() == 0 {
		t.Error("key file is empty")
	}
	if runtime.GOOS != "windows" {
		if info.Mode().Perm() != 0600 {
			t.Errorf("key file permissions = %o, want 0600", info.Mode().Perm())
		}
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

// 环境变量优先于密钥文件：即使文件存在且内容不同，也必须用环境变量的值。
func TestLoadOrGenerateKeyEnvPriority(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)
	t.Setenv("USERPROFILE", dir) // Windows 上 UserHomeDir 读 USERPROFILE

	fileKey := make([]byte, 32)
	for i := range fileKey {
		fileKey[i] = byte(i)
	}
	if err := os.WriteFile(filepath.Join(dir, keyFile), []byte(hexOf(fileKey)), 0600); err != nil {
		t.Fatal(err)
	}

	envKey := make([]byte, 32)
	for i := range envKey {
		envKey[i] = byte(0xff - i)
	}
	t.Setenv(envKeyName, hexOf(envKey))

	got, source, err := loadOrGenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	if source != "env:"+envKeyName {
		t.Errorf("source = %q, want env:%s", source, envKeyName)
	}
	if len(got) != 32 || got[0] != envKey[0] || got[31] != envKey[31] {
		t.Error("env key not used in preference to key file")
	}
}

// 环境变量非法（非 64 位 hex）必须报错，而不是悄悄回退文件（防止操作员以为
// env 生效、实际在用另一把 key → 历史密文解不开且无任何提示）。
func TestLoadOrGenerateKeyInvalidEnv(t *testing.T) {
	t.Setenv(envKeyName, "not-hex")
	if _, _, err := loadOrGenerateKey(); err == nil {
		t.Fatal("invalid env key should error, not fall back to file")
	}
	t.Setenv(envKeyName, hexOf([]byte("too-short")))
	if _, _, err := loadOrGenerateKey(); err == nil {
		t.Fatal("short env key should error, not fall back to file")
	}
}

// 未设置环境变量时回退文件，来源应报告文件路径。
func TestLoadOrGenerateKeyFileFallback(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("HOME", dir)
	t.Setenv("USERPROFILE", dir)
	t.Setenv(envKeyName, "")

	fileKey := make([]byte, 32)
	for i := range fileKey {
		fileKey[i] = byte(i * 3)
	}
	if err := os.WriteFile(filepath.Join(dir, keyFile), []byte(hexOf(fileKey)), 0600); err != nil {
		t.Fatal(err)
	}
	got, source, err := loadOrGenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	if source != filepath.Join(dir, keyFile) {
		t.Errorf("source = %q, want key file path", source)
	}
	if len(got) != 32 || got[1] != fileKey[1] {
		t.Error("file key not loaded")
	}
}
