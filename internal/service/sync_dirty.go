package service

// 端侧"脏"标记与中心可写性探测。
//
// 背景：代理端可在面板上本地增删改（应急，写本地 SQLite，不推中心）；
// 管理端直写中心，不存在分歧。本文件解决"改完之后给提示"：
//   - 脏标记（settings.sync_dirty*）：任一面板定义写落在本地、且中心可达
//     （已配同步或面板存了 key）、且当前非管理模式时置位；拉取应用 / 推送
//     成功后清除。
//   - 拦截点是 Store 接缝的本地实现包装（edgeStore）：30 处面板写调用全经
//     store.A()，而网关运行时（故障转移/恢复/遥测）走 db 直写，天然区分，
//     无误报。db.Get().Xxx 直写的 4 处全是运行时状态（恢复可用/清失败标记），
//     不进脏标记。
//   - onEdgeDirty 钩子：默认落库 + 记系统日志；单测可替换为记录器（db.Get()
//     在 service 单测里为 nil，默认实现 nil-safe 直接跳过）。

import (
	"strings"
	"sync/atomic"
	"time"

	"gateway/internal/db"
	"gateway/internal/models"
	"gateway/internal/store"
	"gateway/internal/supabase"
)

// 脏标记 settings 键。
const (
	settingSyncDirty       = "sync_dirty"        // "1" = 端侧有未同步到中心的本地修改
	settingSyncDirtyAt     = "sync_dirty_at"     // RFC3339，首次置脏时间
	settingSyncDirtyReason = "sync_dirty_reason" // 置脏原因（最后一次定义写）
	settingSyncLastMerge   = "sync_last_merge"   // 最近一次并集摘要 JSON（面板横幅用）
)

// onEdgeDirty 是脏标记的实际落点，默认写 settings + 系统日志。
// 单测替换它以避免依赖 db。
var onEdgeDirty = defaultOnEdgeDirty

// edgeDirtyGen 是脏标记的代际计数：每次置脏 +1。合并/回推这种长操作在清脏
// 前比对代际，期间有新修改则保留脏标记，不静默吞掉用户的最新修改。
var edgeDirtyGen atomic.Int64

func defaultOnEdgeDirty(reason string) {
	d := db.Get()
	if d == nil {
		return
	}
	prev, _ := d.GetSetting(settingSyncDirty)
	now := time.Now().Format(time.RFC3339)
	_ = d.SetSetting(settingSyncDirty, "1")
	_ = d.SetSetting(settingSyncDirtyReason, reason)
	if prev != "1" {
		_ = d.SetSetting(settingSyncDirtyAt, now)
		sbSystemLog("warn", "端侧配置已修改（"+reason+"），与中心未同步"+
			"：版本一致时保留本地生效；中心有新版本时会并集合并（冲突以本地为准）")
	}
}

// clearEdgeDirty 清除脏标记（拉取应用 / 推送成功后调用）。nil-safe。
func clearEdgeDirty() {
	d := db.Get()
	if d == nil {
		return
	}
	_ = d.SetSetting(settingSyncDirty, "")
	_ = d.SetSetting(settingSyncDirtyAt, "")
	_ = d.SetSetting(settingSyncDirtyReason, "")
}

// clearEdgeDirtyIfUnchanged 仅当自 gen 起无新修改时清脏，返回是否清掉。
func clearEdgeDirtyIfUnchanged(gen int64) bool {
	if edgeDirtyGen.Load() != gen {
		return false
	}
	clearEdgeDirty()
	return true
}

// edgeDirtyState 读脏标记。nil-safe（db 未就绪返回干净，避免面板误报）。
func edgeDirtyState() (dirty bool, at, reason string) {
	d := db.Get()
	if d == nil {
		return false, "", ""
	}
	v, _ := d.GetSetting(settingSyncDirty)
	if v != "1" {
		return false, "", ""
	}
	at, _ = d.GetSetting(settingSyncDirtyAt)
	reason, _ = d.GetSetting(settingSyncDirtyReason)
	return true, at, reason
}

// centerKnown 报告端侧是否知道中心的存在（配了同步或面板存了 key）。
// 离线模式（都不知道）下脏标记无意义，markEdgeDirty 直接跳过。
// db 未就绪（单测）一律按"不知道"处理。
func centerKnown() bool {
	_, _, configured := syncSnapshot()
	if configured {
		return true
	}
	if db.Get() == nil {
		return false
	}
	_, _, ok := loadSavedSBConfig()
	return ok
}

// shouldMarkDirty 脏标记总闸（纯函数，可单测）：管理模式直写中心无分歧、
// 离线模式无中心可言，两者都不标。
func shouldMarkDirty(manage, known bool) bool {
	return !manage && known
}

// markEdgeDirty 因 reason 标记端侧脏。管理模式直写中心、无分歧，跳过；
// 离线模式无中心可言，跳过。
func markEdgeDirty(reason string) {
	if !shouldMarkDirty(manageMode.Load(), centerKnown()) {
		return
	}
	edgeDirtyGen.Add(1)
	onEdgeDirty(reason)
}

// probeCenterWritability 探测中心 key 是否可写（管理端级别）。
// key/URL 优先用面板保存的，其次用运行时同步配置。返回 (writable, url, key)。
// 失败/只读一律 writable=false，不抛错（调用方按只读处理即可）。
func probeCenterWritability() (writable bool, sbURL, sbKey string) {
	// 面板保存优先（db 未就绪时跳过该分支，单测/启动早期不 panic）。
	if db.Get() != nil {
		if u, k, ok := loadSavedSBConfig(); ok {
			sbURL, sbKey = u, k
		}
	}
	if sbURL == "" {
		client, sourceURL, configured := syncSnapshot()
		if !configured || client == nil {
			return false, "", ""
		}
		sbURL = strings.TrimSuffix(sourceURL, "/rest/v1/rpc/get_bundle")
		sbKey = strings.TrimSpace(client.AnonKey)
		if sbKey == "" {
			return false, "", ""
		}
	}
	role, _, err := probeSBRole(sbURL, sbKey)
	if err != nil {
		return false, sbURL, sbKey
	}
	return role == sbRoleManagement, sbURL, sbKey
}

// transientCenterStore 用可写 key 建一次性中心 Store（只用于回推，不切换
// 运行模式、不翻 manageMode）。调用方已用 probeCenterWritability 确认可写。
func transientCenterStore(sbURL, sbKey string) (*supabase.Store, error) {
	return supabase.New(supabase.Config{
		URL:        sbURL,
		ServiceKey: sbKey,
	}, nil)
}

// versionsInSync 版本口径的一致判定（不含脏标记，见 edgeInSync）。
func versionsInSync(centerVer, localVer int64) bool {
	return centerVer != 0 && centerVer == localVer
}

// edgeInSync 端侧"已同步"判定：版本一致且本地无未同步修改。
// 脏（本地独有内容）即使版本号碰巧相同也不算同步——版本号只反映中心进度。
func edgeInSync(centerVer, localVer int64) bool {
	if !versionsInSync(centerVer, localVer) {
		return false
	}
	dirty, _, _ := edgeDirtyState()
	return !dirty
}

// syncMerging 报告是否有合并/回推正在后台执行（面板"合并中…"指示用）。
var syncMerging atomic.Bool

func setMerging(v bool) { syncMerging.Store(v) }

// edgeStore 包装本地 Store：面板定义写成功后标脏。管理模式下本包装已被
// supabase.Store 替换（store.Use），不会误标；网关运行时写走 db 直写，
// 不经过 Store 接缝，不会误标。
type edgeStore struct {
	store.Store
}

func wrapEdgeStore(local *db.DB) *edgeStore {
	return &edgeStore{Store: local}
}

func (s *edgeStore) mark(reason string) { markEdgeDirty(reason) }

func (s *edgeStore) CreatePlatform(p *models.Platform) error {
	if err := s.Store.CreatePlatform(p); err != nil {
		return err
	}
	s.mark("新增平台")
	return nil
}

func (s *edgeStore) UpdatePlatform(p *models.Platform) error {
	if err := s.Store.UpdatePlatform(p); err != nil {
		return err
	}
	s.mark("修改平台")
	return nil
}

func (s *edgeStore) DeletePlatform(id int64) error {
	if err := s.Store.DeletePlatform(id); err != nil {
		return err
	}
	s.mark("删除平台")
	return nil
}

func (s *edgeStore) DeletePlatformCascade(id int64) error {
	if err := s.Store.DeletePlatformCascade(id); err != nil {
		return err
	}
	s.mark("删除平台")
	return nil
}

func (s *edgeStore) SetPlatformEnabled(id int64, enabled bool) error {
	if err := s.Store.SetPlatformEnabled(id, enabled); err != nil {
		return err
	}
	s.mark("切换平台开关")
	return nil
}

func (s *edgeStore) SetPlatformSortOrder(ids []int64) error {
	if err := s.Store.SetPlatformSortOrder(ids); err != nil {
		return err
	}
	s.mark("平台排序")
	return nil
}

func (s *edgeStore) UpdatePlatformFormats(platformID int64, formatsJSON string, propagateToRAPIs bool, formatEndpointsJSON string) error {
	if err := s.Store.UpdatePlatformFormats(platformID, formatsJSON, propagateToRAPIs, formatEndpointsJSON); err != nil {
		return err
	}
	s.mark("平台协议探测")
	return nil
}

func (s *edgeStore) SetPlatformKeys(platformID int64, keys []models.PlatformKey) error {
	if err := s.Store.SetPlatformKeys(platformID, keys); err != nil {
		return err
	}
	s.mark("批量保存密钥")
	return nil
}

func (s *edgeStore) AddPlatformKey(k *models.PlatformKey) error {
	if err := s.Store.AddPlatformKey(k); err != nil {
		return err
	}
	s.mark("新增密钥")
	return nil
}

func (s *edgeStore) UpdatePlatformKey(k *models.PlatformKey) error {
	if err := s.Store.UpdatePlatformKey(k); err != nil {
		return err
	}
	s.mark("修改密钥")
	return nil
}

func (s *edgeStore) DeletePlatformKey(keyID int64) error {
	if err := s.Store.DeletePlatformKey(keyID); err != nil {
		return err
	}
	s.mark("删除密钥")
	return nil
}

func (s *edgeStore) DetachKeyFromRAPIs(keyID int64) ([]string, error) {
	removed, err := s.Store.DetachKeyFromRAPIs(keyID)
	if err != nil {
		return removed, err
	}
	if len(removed) > 0 {
		s.mark("解绑密钥")
	}
	return removed, nil
}

func (s *edgeStore) CreateRAPI(r *models.RAPI) error {
	if err := s.Store.CreateRAPI(r); err != nil {
		return err
	}
	s.mark("新增模型")
	return nil
}

func (s *edgeStore) UpdateRAPI(r *models.RAPI) error {
	if err := s.Store.UpdateRAPI(r); err != nil {
		return err
	}
	s.mark("修改模型")
	return nil
}

func (s *edgeStore) DeleteRAPI(id int64) error {
	if err := s.Store.DeleteRAPI(id); err != nil {
		return err
	}
	s.mark("删除模型")
	return nil
}

func (s *edgeStore) DeleteRAPICascade(id int64) error {
	if err := s.Store.DeleteRAPICascade(id); err != nil {
		return err
	}
	s.mark("删除模型")
	return nil
}

func (s *edgeStore) SetRAPIEnabled(id int64, enabled bool) error {
	if err := s.Store.SetRAPIEnabled(id, enabled); err != nil {
		return err
	}
	s.mark("切换模型开关")
	return nil
}

func (s *edgeStore) SetRAPISortOrder(ids []int64) error {
	if err := s.Store.SetRAPISortOrder(ids); err != nil {
		return err
	}
	s.mark("模型排序")
	return nil
}

func (s *edgeStore) UpdateRAPIFormats(id int64, formatsJSON string) error {
	if err := s.Store.UpdateRAPIFormats(id, formatsJSON); err != nil {
		return err
	}
	s.mark("模型协议探测")
	return nil
}

func (s *edgeStore) UpdateRAPIHeaders(id int64, headersJSON string) error {
	if err := s.Store.UpdateRAPIHeaders(id, headersJSON); err != nil {
		return err
	}
	s.mark("模型自定义头")
	return nil
}

func (s *edgeStore) CreateLAPI(u *models.LAPI) error {
	if err := s.Store.CreateLAPI(u); err != nil {
		return err
	}
	s.mark("新增接口")
	return nil
}

func (s *edgeStore) UpdateLAPI(l *models.LAPI) error {
	if err := s.Store.UpdateLAPI(l); err != nil {
		return err
	}
	s.mark("修改接口")
	return nil
}

func (s *edgeStore) DeleteLAPI(id int64) error {
	if err := s.Store.DeleteLAPI(id); err != nil {
		return err
	}
	s.mark("删除接口")
	return nil
}

func (s *edgeStore) SetLAPIEnabled(id int64, enabled bool) error {
	if err := s.Store.SetLAPIEnabled(id, enabled); err != nil {
		return err
	}
	s.mark("切换接口开关")
	return nil
}

func (s *edgeStore) SetLAPIRAPIOrder(lapiID int64, rapiIDs []int64) error {
	if err := s.Store.SetLAPIRAPIOrder(lapiID, rapiIDs); err != nil {
		return err
	}
	s.mark("路由链调整")
	return nil
}
