package crypto

import (
	"strings"
	"testing"
)

// ParseKey：64 位 hex → 32 字节；短/长/非法一律报错。
// center_key 配错时必须在启动即暴露，而不是写中心时才报。
func TestParseKey(t *testing.T) {
	good := strings.Repeat("ab", 32) // 64 hex chars = 32 bytes
	k, err := ParseKey(good)
	if err != nil {
		t.Fatalf("ParseKey(valid): %v", err)
	}
	if len(k) != 32 || k[0] != 0xab || k[31] != 0xab {
		t.Errorf("ParseKey decoded wrong: len=%d first=%x", len(k), k[0])
	}
	for _, bad := range []string{"", "not-hex!!", strings.Repeat("ab", 8), strings.Repeat("ab", 33), "  "} {
		if _, err := ParseKey(bad); err == nil {
			t.Errorf("ParseKey(%q) should fail", bad)
		}
	}
	// 首尾空白容忍（从配置文件复制常带换行）。
	k2, err := ParseKey("  " + good + "\n")
	if err != nil || len(k2) != 32 {
		t.Errorf("ParseKey with surrounding whitespace: %v", err)
	}
}

// DecryptWithKey 显式 key 路径：无 enc: 前缀按明文透传（中心明文模式），
// 有前缀但 key 错 → 报错（串 key 时必须炸，而不是返回垃圾）。
func TestDecryptWithKeyPaths(t *testing.T) {
	initTestKey(t)
	mu.RLock()
	key := append([]byte(nil), activeKey...)
	mu.RUnlock()

	plain := "sk-plain-in-center"
	if got, err := DecryptWithKey(plain, key); err != nil || got != plain {
		t.Errorf("plaintext passthrough = %q,%v; want %q,nil", got, err, plain)
	}
	// 空 key + 明文也透传（本地未初始化但读到明文行时不挡路）。
	if got, err := DecryptWithKey(plain, nil); err != nil || got != plain {
		t.Errorf("nil-key passthrough = %q,%v", got, err)
	}

	enc, err := EncryptWithKey("secret-value", key)
	if err != nil {
		t.Fatalf("EncryptWithKey: %v", err)
	}
	if got, err := DecryptWithKey(enc, key); err != nil || got != "secret-value" {
		t.Errorf("roundtrip = %q,%v", got, err)
	}
	wrong := append([]byte(nil), key...)
	wrong[0] ^= 0xff
	if _, err := DecryptWithKey(enc, wrong); err == nil {
		t.Error("decrypt with wrong key should fail, not return garbage")
	}
}

// 未初始化时加解密必须显式报错（"key not initialised"），而不是 panic 或
// 返回空值让调用方误以为成功。
func TestCryptoWithoutInitErrors(t *testing.T) {
	mu.Lock()
	saved := activeKey
	activeKey = nil
	mu.Unlock()
	defer func() {
		mu.Lock()
		activeKey = saved
		mu.Unlock()
	}()

	if _, err := Encrypt("x"); err == nil {
		t.Error("Encrypt without init should fail")
	} else if !strings.Contains(err.Error(), "not initialised") {
		t.Errorf("Encrypt error = %q, want key-not-initialised", err)
	}
	if _, err := Decrypt("enc:deadbeef"); err == nil {
		t.Error("Decrypt without init should fail")
	}
}
