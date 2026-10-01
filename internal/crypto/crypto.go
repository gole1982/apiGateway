// Package crypto provides AES-256-GCM encryption for sensitive values stored in the database.
//
// 主密钥来源：**只有环境变量 APIGATEWAY_KEY**（64 位 hex = 32 字节）。
//
// 为什么不再有 ~/.apiGateway.key 兜底（2026-09 移除）：
//
//  1. 容器里它是定时炸弹。文件写在容器可写层，docker compose down / 重建即
//     消失；旧代码此时会**静默生成一把新随机 key**并继续启动，于是所有历史
//     密文（平台 token、settings.sb_api_key）集体解不开，
//     且要等到某次重建才暴露。改成"缺 key 即启动失败"，把这个静默损坏
//     提前到部署那一刻暴露。
//  2. 它没有增加安全性，只是把明文密钥从环境变量搬到了磁盘。
//
// 丢 key 的后果与恢复路径：定义类数据（平台 token / Key /
// 模型 / 接口）可从中心（Supabase）重新拉回 —— center_key 留空时中心存明文
// （见 internal/db/bundle_export.go 的 reencryptLocal）；不可恢复的只有
// 本地遥测（request_logs / rapi_metrics / sessions 等，从不同步）与
// settings 里的中心连接凭据本身（sb_api_key / sb_center_key，需从 Supabase
// 控制台重新录入后才能再连中心）。详见 docs/日常操作手册.md「换密钥」。
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
	envKeyName = "APIGATEWAY_KEY" // 64 位 hex（32 字节），唯一的主密钥来源
	// legacyKeyFile 是 2026-09 之前的主密钥文件名。它已不再被读取，仅用于
	// 在错误信息里告诉旧部署用户"这把 key 搬到环境变量里去"。
	legacyKeyFile = ".apiGateway.key"
)

var (
	mu        sync.RWMutex
	activeKey []byte // 32 bytes
	keySource string // "env:APIGATEWAY_KEY"（供启动日志确认生效路径）
)

// Init loads the master key from the APIGATEWAY_KEY environment variable.
// Must be called once at startup before any Encrypt/Decrypt calls.
//
// 缺少或格式错误时返回错误，调用方应据此**拒绝启动** —— 见 loadKey 的说明。
func Init() error {
	key, source, err := loadKey()
	if err != nil {
		return fmt.Errorf("crypto init: %w", err)
	}
	mu.Lock()
	activeKey = key
	keySource = source
	mu.Unlock()
	return nil
}

// KeySource 报告主密钥来源（恒为 "env:APIGATEWAY_KEY"），供启动日志确认生效
// 路径 —— 避免操作员以为环境变量生效、实际却在用别的东西。
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

// loadKey 解析主密钥：只认环境变量 APIGATEWAY_KEY。
//
// 不再回退到 ~/.apiGateway.key，也不再自动生成 —— 见包注释的 rationale。
// 缺失时报错而非兜底是刻意的：启动失败要发生在部署那一刻，且错误信息本身
// 要能指导用户完成迁移（errMissingEnv 的文案里带迁移命令）。
func loadKey() ([]byte, string, error) {
	v := strings.TrimSpace(os.Getenv(envKeyName))
	if v == "" {
		return nil, "", fmt.Errorf("%w：未设置环境变量 %s。生成一把：openssl rand -hex 32%s",
			errMissingEnv, envKeyName, legacyKeyHint())
	}
	key, err := hex.DecodeString(v)
	if err != nil {
		return nil, "", fmt.Errorf("%s 必须是 64 位 hex（32 字节），当前长度 %d: %w", envKeyName, len(v), err)
	}
	if len(key) != 32 {
		return nil, "", fmt.Errorf("%s 必须是 32 字节（64 位 hex），解出 %d 字节", envKeyName, len(key))
	}
	return key, "env:" + envKeyName, nil
}

// legacyKeyFilePath 返回遗留密钥文件路径。**仅用于错误提示**，它永远不会被
// 当作主密钥读取 —— 让从旧版升级的用户一眼看到该迁移哪把 key。
func legacyKeyFilePath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, legacyKeyFile)
}

// legacyKeyHint 在检测到遗留密钥文件时，追加一句"把旧 key 搬到环境变量"的
// 指引 —— 这是旧部署升级时唯一要做的事，写进错误信息比让人去翻文档可靠。
// 没有文件（或读不到 home）时返回空串，不影响主流程。
func legacyKeyHint() string {
	p := legacyKeyFilePath()
	if p == "" {
		return ""
	}
	data, err := os.ReadFile(p)
	if err != nil {
		return ""
	}
	if key, decErr := hex.DecodeString(strings.TrimSpace(string(data))); decErr != nil || len(key) != 32 {
		return ""
	}
	return fmt.Sprintf("\n检测到旧版密钥文件 %s —— 沿用它以免历史密文解不开：\n"+
		"  export %s=$(cat %q)   # bash/zsh\n"+
		"  # PowerShell: $env:%s = Get-Content %q",
		p, envKeyName, p, envKeyName, p)
}

// errMissingEnv 让"没配 key"可被 errors.Is 判定，与"key 格式错"区分开。
var errMissingEnv = errors.New("missing master key")


