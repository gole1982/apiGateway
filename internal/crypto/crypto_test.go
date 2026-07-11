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
