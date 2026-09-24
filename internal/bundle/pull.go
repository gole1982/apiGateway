package bundle

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// ErrNoChange 表示中心的 version 与代理当前一致（get_bundle 返回 null），
// 调用方应跳过 Apply。等价于 HTTP 304 短路。
var ErrNoChange = errors.New("bundle: no change")

// Client 拉取中心 Supabase 的定义快照。
//
// 字段对应 proxy.cfg [sync]：
//   - BundleURL  = .../rest/v1/rpc/get_bundle
//   - VersionURL = .../rest/v1/rpc/get_version
//   - AnonKey    = Supabase anon 公钥（只读，可放代理端）
type Client struct {
	BundleURL  string
	VersionURL string
	AnonKey    string
	HTTP       *http.Client
}

// NewClient 构造一个默认超时 15s 的拉取客户端。
func NewClient(bundleURL, versionURL, anonKey string) *Client {
	return &Client{
		BundleURL:  strings.TrimSpace(bundleURL),
		VersionURL: strings.TrimSpace(versionURL),
		AnonKey:    strings.TrimSpace(anonKey),
		HTTP:       &http.Client{Timeout: 15 * time.Second},
	}
}

// PullVersion 拉取中心当前版本号（一个 BIGINT，便宜，代理轮询只调它）。
func (c *Client) PullVersion(ctx context.Context) (int64, error) {
	if c.VersionURL == "" {
		return 0, errors.New("bundle: version_url not configured")
	}
	body, err := c.get(ctx, c.VersionURL, nil)
	if err != nil {
		return 0, fmt.Errorf("pull version: %w", err)
	}
	var v int64
	if err := json.Unmarshal(body, &v); err != nil {
		return 0, fmt.Errorf("pull version: parse %q: %w", truncate(body), err)
	}
	return v, nil
}

// PullBundle 拉取完整定义快照。currentVersion 与中心一致时返回 ErrNoChange。
func (c *Client) PullBundle(ctx context.Context, currentVersion int64) (*Envelope, error) {
	if c.BundleURL == "" {
		return nil, errors.New("bundle: source_url not configured")
	}
	q := url.Values{}
	q.Set("p_version", strconv.FormatInt(currentVersion, 10))
	body, err := c.get(ctx, c.BundleURL, q)
	if err != nil {
		return nil, fmt.Errorf("pull bundle: %w", err)
	}
	trimmed := strings.TrimSpace(string(body))
	if trimmed == "" || trimmed == "null" {
		return nil, ErrNoChange
	}
	env, err := Parse(body)
	if err != nil {
		return nil, fmt.Errorf("pull bundle: %w", err)
	}
	return env, nil
}

// get 发起一次带 Supabase 鉴权头的 GET。
func (c *Client) get(ctx context.Context, raw string, q url.Values) ([]byte, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("bad url %q: %w", raw, err)
	}
	if q != nil {
		u.RawQuery = q.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	if c.AnonKey != "" {
		req.Header.Set("apikey", c.AnonKey)
		req.Header.Set("Authorization", "Bearer "+c.AnonKey)
	}
	req.Header.Set("Accept", "application/json")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20)) // 32MB 上限，防异常包打爆内存
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("http %d: %s", resp.StatusCode, truncate(body))
	}
	return body, nil
}

func truncate(b []byte) string {
	const max = 200
	if len(b) <= max {
		return string(b)
	}
	return string(b[:max]) + "…"
}
