package service

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os/exec"
	"strings"
	"sync"
	"time"

	"gateway/internal/logger"
)

// -----------------------------------------------------------------------------
// 版本更新检查 —— 网关主动轮询 GitHub 最新 tag，与本地 Version 对比。
// 服务器在内网（RFC 1918），GitHub 无法主动推消息，所以由网关自己拉。
// -----------------------------------------------------------------------------

const (
	// GitHub API：列仓库所有 tag（不需要认证，匿名限额 60 次/小时）。
	// 轮询间隔 10 分钟 → 每天 144 次，远低于限额。
	githubTagsURL = "https://api.github.com/repos/gole1982/apiGateway/tags?per_page=10"
	// GitHub Release 说明：取最新 tag 对应的 release（有则带 body/changelog）。
	githubReleaseURL = "https://api.github.com/repos/gole1982/apiGateway/releases/latest"
	checkInterval    = 10 * time.Minute
	requestTimeout   = 10 * time.Second
)

// UpdateInfo 是 /api/update/check 返回给前端的结构。
type UpdateInfo struct {
	CurrentVersion  string `json:"current_version"` // 本地运行版本（ldflags 注入）
	LatestVersion   string `json:"latest_version"`  // GitHub 最新 tag（去掉 v 前缀）
	UpdateAvailable bool   `json:"update_available"`
	ReleaseNotes    string `json:"release_notes,omitempty"` // release body（markdown）
	ReleaseURL      string `json:"release_url,omitempty"`   // GitHub release 页面
	CheckedAt       string `json:"checked_at"`              // 上次检查时间（RFC3339）
	// 以下用于前端展示更新日志（从当前版本到最新版本之间的 commit 列表）
	Commits     []CommitInfo `json:"commits,omitempty"`
	Updating    bool         `json:"updating"`               // 正在执行更新
	UpdateError string       `json:"update_error,omitempty"` // 上次更新失败原因
}

// CommitInfo 简要展示两个版本之间的提交。
type CommitInfo struct {
	SHA     string `json:"sha"`
	Message string `json:"message"`
	Date    string `json:"date"`
}

// githubTag 是 GitHub tags API 返回的单个 tag。
type githubTag struct {
	Name string `json:"name"`
}

// githubRelease 是 GitHub releases API 返回的最新 release。
type githubRelease struct {
	TagName     string `json:"tag_name"`
	Name        string `json:"name"`
	Body        string `json:"body"`
	HTMLURL     string `json:"html_url"`
	PublishedAt string `json:"published_at"`
}

var (
	updateMu     sync.RWMutex
	cachedUpdate UpdateInfo
	lastCheckErr error
	updateState  string // "" | "updating" | "failed" | "success"
	updateErrMsg string
	updateCtrl   context.CancelFunc // 更新执行上下文的 cancel（用于超时取消）
)

// startUpdateChecker 启动后台 goroutine 定时检查 GitHub 最新版本。
// 在 ServeContext 中调用（见 service.go）。
func startUpdateChecker(ctx context.Context) {
	// 立即检查一次（启动后马上知道是否需要更新）。
	doCheck()

	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			doCheck()
		}
	}
}

// doCheck 执行一次 GitHub API 轮询，更新 cachedUpdate。
func doCheck() {
	// 本地是 dev 构建时，跳过版本检查（没有有效的 tag 可比）。
	if Version == "dev" || Version == "" {
		updateMu.Lock()
		cachedUpdate = UpdateInfo{
			CurrentVersion:  Version,
			LatestVersion:   "",
			UpdateAvailable: false,
			CheckedAt:       time.Now().Format(time.RFC3339),
		}
		updateMu.Unlock()
		return
	}

	current := normalizeVersion(Version)
	latest, releaseBody, releaseURL, err := fetchLatestRelease()
	if err != nil {
		logger.DefaultConsole().Warn("service", "[UPDATE] 版本检查失败", "err", err)
		lastCheckErr = err
		// 保留上次的缓存，不覆盖——避免网络抖动导致提示消失。
		return
	}

	latestNorm := normalizeVersion(latest)
	available := compareVersions(current, latestNorm) < 0

	var commits []CommitInfo
	if available {
		commits = fetchCommits(current, latestNorm)
	}

	updateMu.Lock()
	cachedUpdate = UpdateInfo{
		CurrentVersion:  Version,
		LatestVersion:   latest,
		UpdateAvailable: available,
		ReleaseNotes:    releaseBody,
		ReleaseURL:      releaseURL,
		CheckedAt:       time.Now().Format(time.RFC3339),
		Commits:         commits,
		Updating:        updateState == "updating",
		UpdateError:     updateErrMsg,
	}
	updateMu.Unlock()
	lastCheckErr = nil
}

// fetchLatestRelease 从 GitHub 拉最新 tag + release 说明。
// 返回 tag 名（带 v 前缀）、release body、release URL。
func fetchLatestRelease() (tag, body, url string, err error) {
	client := &http.Client{Timeout: requestTimeout}

	// 1. 取最新 tag 名
	resp, err := client.Get(githubTagsURL)
	if err != nil {
		return "", "", "", fmt.Errorf("获取 tags 失败: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return "", "", "", fmt.Errorf("tags API 返回 HTTP %d", resp.StatusCode)
	}
	var tags []githubTag
	if err := json.NewDecoder(resp.Body).Decode(&tags); err != nil {
		return "", "", "", fmt.Errorf("解析 tags 响应失败: %w", err)
	}
	if len(tags) == 0 {
		return "", "", "", fmt.Errorf("仓库没有任何 tag")
	}
	tag = tags[0].Name // 例如 "v1.6.20261004"

	// 2. 取对应 release（可能有也可能没有）
	releaseResp, err := client.Get(githubReleaseURL)
	if err != nil {
		// release 拉不到不影响版本对比，只记日志。
		return tag, "", "", nil
	}
	defer releaseResp.Body.Close()
	if releaseResp.StatusCode == 200 {
		var rel githubRelease
		if json.NewDecoder(releaseResp.Body).Decode(&rel) == nil {
			return tag, rel.Body, rel.HTMLURL, nil
		}
	}

	return tag, "", "", nil
}

// fetchCommits 拉取两个 tag 之间的提交（用 compare API）。
// 只取前 20 条用于前端展示。
func fetchCommits(from, to string) []CommitInfo {
	// GitHub compare API: ...from...to... （都需要带 v 前缀）
	fromTag := ensureVPrefix(from)
	toTag := ensureVPrefix(to)
	url := fmt.Sprintf("https://api.github.com/repos/gole1982/apiGateway/compare/%s...%s?per_page=20", fromTag, toTag)

	client := &http.Client{Timeout: requestTimeout}
	resp, err := client.Get(url)
	if err != nil || resp.StatusCode != 200 {
		return nil
	}
	defer resp.Body.Close()

	var cmp struct {
		Commits []struct {
			SHA    string `json:"sha"`
			Commit struct {
				Message string `json:"message"`
				Author  struct {
					Date string `json:"date"`
				} `json:"author"`
			} `json:"commit"`
		} `json:"commits"`
	}
	if json.NewDecoder(resp.Body).Decode(&cmp) != nil {
		return nil
	}

	var result []CommitInfo
	for _, c := range cmp.Commits {
		// 只取 commit message 第一行
		msg := c.Commit.Message
		if idx := strings.IndexByte(msg, '\n'); idx > 0 {
			msg = msg[:idx]
		}
		shortSHA := c.SHA
		if len(shortSHA) > 7 {
			shortSHA = shortSHA[:7]
		}
		result = append(result, CommitInfo{
			SHA:     shortSHA,
			Message: msg,
			Date:    c.Commit.Author.Date,
		})
	}
	return result
}

// normalizeVersion 去掉 v 前缀，返回纯数字版本号。
// "v1.6.20261004" → "1.6.20261004"
// "1.6.20261004"  → "1.6.20261004"
// "v1.6.20261004-2-gabc" → "1.6.20261004" （git describe 的后缀截掉）
func normalizeVersion(v string) string {
	v = strings.TrimPrefix(v, "v")
	// git describe 格式：v1.6.20261004-2-gabc1234 → 取第一个 - 之前
	if idx := strings.IndexByte(v, '-'); idx > 0 {
		v = v[:idx]
	}
	return v
}

func ensureVPrefix(v string) string {
	if !strings.HasPrefix(v, "v") {
		return "v" + v
	}
	return v
}

// compareVersions 比较两个版本字符串。
// 返回 -1 (a<b), 0 (a==b), 1 (a>b)。
// 版本格式：major.minor.yyyymmdd，例如 1.6.20261004
func compareVersions(a, b string) int {
	pa := strings.Split(a, ".")
	pb := strings.Split(b, ".")
	maxLen := len(pa)
	if len(pb) > maxLen {
		maxLen = len(pb)
	}
	for i := 0; i < maxLen; i++ {
		var na, nb int
		if i < len(pa) {
			fmt.Sscanf(pa[i], "%d", &na)
		}
		if i < len(pb) {
			fmt.Sscanf(pb[i], "%d", &nb)
		}
		if na < nb {
			return -1
		}
		if na > nb {
			return 1
		}
	}
	return 0
}

// getUpdateInfo 返回当前缓存的更新信息（线程安全）。
func getUpdateInfo() UpdateInfo {
	updateMu.RLock()
	defer updateMu.RUnlock()
	return cachedUpdate
}

// applyUpdate 在服务器上执行 update.sh。
// 此函数会阻塞直到更新完成或超时（5 分钟）。
func applyUpdate() error {
	updateMu.Lock()
	if updateState == "updating" {
		updateMu.Unlock()
		return fmt.Errorf("更新正在进行中")
	}
	updateState = "updating"
	updateErrMsg = ""
	updateMu.Unlock()

	// 更新完成后清理状态。
	defer func() {
		updateMu.Lock()
		if updateState == "updating" {
			updateState = "" // 正常退出时清空
		}
		updateMu.Unlock()
	}()

	// 用 context 控制超时（5 分钟）。
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	updateMu.Lock()
	updateCtrl = cancel
	updateMu.Unlock()

	// 执行 update.sh —— 它会 git pull + docker compose pull + up -d + 健康检查。
	// 注意：这个进程自己就在容器里跑，docker compose up -d 会重建容器，
	// 当前进程会被杀掉。所以这个 HTTP 请求大概率不会收到正常返回。
	// 前端需要用超时 + 轮询 /api/update/check 来判断更新是否成功。
	cmd := exec.CommandContext(ctx, "bash", "update.sh")
	output, err := cmd.CombinedOutput()
	if err != nil {
		updateMu.Lock()
		updateState = "failed"
		updateErrMsg = fmt.Sprintf("更新失败: %v\n输出: %s", err, string(output))
		updateMu.Unlock()
		logger.DefaultConsole().Error("service", "[UPDATE] 更新失败", "err", err, "output", string(output))
		return fmt.Errorf("%s", updateErrMsg)
	}

	updateMu.Lock()
	updateState = "success"
	updateMu.Unlock()
	logger.DefaultConsole().Info("service", "[UPDATE] 更新成功", "output", string(output))
	return nil
}
