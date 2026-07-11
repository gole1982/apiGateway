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

// Encrypt encrypts plaintext using AES-256-GCM and returns a hex-encoded ciphertext
// in the format "enc:<nonce_hex><ciphertext_hex>".
// An empty plaintext is returned unchanged (no empty-string ciphertext).
func Encrypt(plaintext string) (string, error) {
	if plaintext == "" {
		return "", nil
	}
	mu.RLock()
	key := activeKey
	mu.RUnlock()
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

// Decrypt decrypts a value produced by Encrypt. Values that do not carry the "enc:" prefix
// are returned as-is (transparent plaintext pass-through for legacy / migration rows).
func Decrypt(ciphertext string) (string, error) {
	if ciphertext == "" {
		return "", nil
	}
	if !strings.HasPrefix(ciphertext, "enc:") {
		// Plaintext legacy value — return unchanged.
		return ciphertext, nil
	}

	mu.RLock()
	key := activeKey
	mu.RUnlock()
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
