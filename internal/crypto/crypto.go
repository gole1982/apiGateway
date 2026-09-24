// Package crypto provides AES-256-GCM encryption for sensitive values stored in the database.
//
// 主密钥来源（优先级从高到低）：
//  1. 环境变量 APIGATEWAY_KEY（64 位 hex = 32 字节）——跨平台（Windows/Ubuntu/Kali
//     通用）、不落地磁盘，推荐生产使用；
//  2. ~/.apiGateway.key 文件（hex，0600）——向后兼容既有部署；环境变量未设置时
//     沿用旧行为（首次运行自动生成）。
//
// 注意：主密钥决定全部 enc: 密文的可解性。从文件切换到环境变量时，必须把文件
// 里的 hex 原样复制到 APIGATEWAY_KEY，否则历史密文（settings.sb_api_key、平台
// token 等）将无法解密。
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

const (
	keyFile    = ".apiGateway.key"
	envKeyName = "APIGATEWAY_KEY" // 64 位 hex（32 字节），优先级高于 keyFile
)

var (
	mu         sync.RWMutex
	activeKey  []byte // 32 bytes
	keySource  string // "env:APIGATEWAY_KEY" 或 keyFile 的绝对路径（Init 后可用）
)

// Init loads the master key: APIGATEWAY_KEY env var first, then ~/.apiGateway.key.
// Must be called once at startup before any Encrypt/Decrypt calls.
func Init() error {
	key, source, err := loadOrGenerateKey()
	if err != nil {
		return fmt.Errorf("crypto init: %w", err)
	}
	mu.Lock()
	activeKey = key
	keySource = source
	mu.Unlock()
	return nil
}

// KeySource 报告主密钥来源（"env:APIGATEWAY_KEY" 或密钥文件路径），供启动日志
// 确认生效路径——避免操作员以为环境变量生效、实际却在用旧文件（或反之）。
// Init 之前调用返回空串。
func KeySource() string {
	mu.RLock()
	defer mu.RUnlock()
	return keySource
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

// loadOrGenerateKey 解析主密钥：环境变量 APIGATEWAY_KEY 优先；未设置则读
// ~/.apiGateway.key，不存在时生成新随机 key 并写入（0600）。
// 返回 (key, 来源描述, error)。
func loadOrGenerateKey() ([]byte, string, error) {
	// 1) 环境变量优先：不落地磁盘，跨平台（无 DPAPI 依赖）。
	if v := strings.TrimSpace(os.Getenv(envKeyName)); v != "" {
		key, err := hex.DecodeString(v)
		if err != nil || len(key) != 32 {
			return nil, "", fmt.Errorf("env %s must be 32-byte hex (64 chars), got %d hex chars", envKeyName, len(v))
		}
		return key, "env:" + envKeyName, nil
	}

	// 2) 密钥文件（向后兼容）。
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, "", fmt.Errorf("resolve home dir: %w", err)
	}
	path := filepath.Join(home, keyFile)

	data, err := os.ReadFile(path)
	if err == nil {
		// Key file exists — decode it.
		key, decErr := hex.DecodeString(strings.TrimSpace(string(data)))
		if decErr != nil || len(key) != 32 {
			return nil, "", fmt.Errorf("invalid key in %s: must be 32-byte hex", path)
		}
		return key, path, nil
	}
	if !os.IsNotExist(err) {
		return nil, "", fmt.Errorf("read key file %s: %w", path, err)
	}

	// Generate a new key.
	key := make([]byte, 32)
	if _, err = io.ReadFull(rand.Reader, key); err != nil {
		return nil, "", fmt.Errorf("generate key: %w", err)
	}

	// Write with owner-only permissions.
	if err = os.WriteFile(path, []byte(hex.EncodeToString(key)), 0600); err != nil {
		return nil, "", fmt.Errorf("write key file %s: %w", path, err)
	}
	return key, path, nil
}
