// Package crypto provides AES-256-GCM encryption for sensitive values stored in the database.
// The encryption key is a 32-byte random key persisted in ~/.apiGateway.key (hex-encoded).
// On first run the key is generated and written to that file; subsequent runs load it from there.
package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const keyFile = ".apiGateway.key"

var (
	mu        sync.RWMutex
	activeKey []byte // 32 bytes
)

// Init loads (or generates) the encryption key from ~/.apiGateway.key.
// Must be called once at startup before any Encrypt/Decrypt calls.
func Init() error {
	key, err := loadOrGenerateKey()
	if err != nil {
		return fmt.Errorf("crypto init: %w", err)
	}
	mu.Lock()
	activeKey = key
	mu.Unlock()
	return nil
}

// Encrypt encrypts plaintext using AES-256-GCM with the local active key and returns a
// hex-encoded ciphertext in the format "enc:<nonce_hex><ciphertext_hex>".
// An empty plaintext is returned unchanged (no empty-string ciphertext).
func Encrypt(plaintext string) (string, error) {
	mu.RLock()
	key := activeKey
	mu.RUnlock()
	return encryptWithKey(plaintext, key)
}

// Decrypt decrypts a value produced by Encrypt (local key). Values that do not carry the
// "enc:" prefix are returned as-is (transparent plaintext pass-through for legacy rows).
func Decrypt(ciphertext string) (string, error) {
	mu.RLock()
	key := activeKey
	mu.RUnlock()
	return decryptWithKey(ciphertext, key)
}

// EncryptWithKey / DecryptWithKey 用显式 key 加解密，供中心库密文 ↔ 本地密文的
// 边界转换：管理端写入中心前用 center_key 加密；代理 Apply 拉到中心密文后用
// center_key 解出明文，再用本地 active key 重新加密入库。格式与 Encrypt/Decrypt 一致。
func EncryptWithKey(plaintext string, key []byte) (string, error) {
	return encryptWithKey(plaintext, key)
}

func DecryptWithKey(ciphertext string, key []byte) (string, error) {
	return decryptWithKey(ciphertext, key)
}

// ParseKey 把 hex 编码的 32 字节密钥文本解析为原始 key（center_key 配置用）。
func ParseKey(hexKey string) ([]byte, error) {
	key, err := hex.DecodeString(strings.TrimSpace(hexKey))
	if err != nil {
		return nil, fmt.Errorf("crypto: parse key: %w", err)
	}
	if len(key) != 32 {
		return nil, fmt.Errorf("crypto: key must be 32 bytes, got %d", len(key))
	}
	return key, nil
}

func encryptWithKey(plaintext string, key []byte) (string, error) {
	if plaintext == "" {
		return "", nil
	}
	if len(key) == 0 {
		return "", errors.New("crypto: key not initialised")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err = io.ReadFull(rand.Reader, nonce); err != nil {
		return "", err
	}
	sealed := gcm.Seal(nonce, nonce, []byte(plaintext), nil)
	return "enc:" + hex.EncodeToString(sealed), nil
}

func decryptWithKey(ciphertext string, key []byte) (string, error) {
	if ciphertext == "" {
		return "", nil
	}
	if !strings.HasPrefix(ciphertext, "enc:") {
		// Plaintext legacy value — return unchanged.
		return ciphertext, nil
	}
	if len(key) == 0 {
		return "", errors.New("crypto: key not initialised")
	}
	data, err := hex.DecodeString(strings.TrimPrefix(ciphertext, "enc:"))
	if err != nil {
		return "", fmt.Errorf("crypto: hex decode: %w", err)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	ns := gcm.NonceSize()
	if len(data) < ns {
		return "", errors.New("crypto: ciphertext too short")
	}
	plaintext, err := gcm.Open(nil, data[:ns], data[ns:], nil)
	if err != nil {
		return "", fmt.Errorf("crypto: decrypt: %w", err)
	}
	return string(plaintext), nil
}

// loadOrGenerateKey reads ~/.apiGateway.key or creates it with a fresh random key.
func loadOrGenerateKey() ([]byte, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, fmt.Errorf("resolve home dir: %w", err)
	}
	path := filepath.Join(home, keyFile)

	data, err := os.ReadFile(path)
	if err == nil {
		// Key file exists — decode it.
		key, decErr := hex.DecodeString(strings.TrimSpace(string(data)))
		if decErr != nil || len(key) != 32 {
			return nil, fmt.Errorf("invalid key in %s: must be 32-byte hex", path)
		}
		return key, nil
	}
	if !os.IsNotExist(err) {
		return nil, fmt.Errorf("read key file %s: %w", path, err)
	}

	// Generate a new key.
	key := make([]byte, 32)
	if _, err = io.ReadFull(rand.Reader, key); err != nil {
		return nil, fmt.Errorf("generate key: %w", err)
	}

	// Write with owner-only permissions.
	if err = os.WriteFile(path, []byte(hex.EncodeToString(key)), 0600); err != nil {
		return nil, fmt.Errorf("write key file %s: %w", path, err)
	}
	return key, nil
}
