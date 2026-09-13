// Package store 是定义类配置的持久层接缝：dashboard 的定义类 CRUD 一律经
// Store 访问，具体落库由当前激活的实现决定：
//
//   - 独立/代理模式：*db.DB（本地 SQLite，行为与历史版本完全一致）
//   - 管理模式：supabase.Store（PostgREST 直写中心，见 [management] 配置）
//
// 运行时读（请求热路径、指标、日志、健康态）**不经本包**，始终读本地 SQLite。
// 设计：docs/superpowers/specs/2026-09-03-center-edge-config-sync-design.md §3.2/§7
package store

import (
	"sync"

	"gateway/internal/db"
	"gateway/internal/models"
)

// Store 覆盖 dashboard 定义类 handler 用到的全部持久化操作。
// 方法签名与 *db.DB 完全一致，因此 *db.DB 天然满足本接口。
type Store interface {
	// ---- platform ----
	GetPlatforms() ([]models.Platform, error)
	GetPlatformByID(id int64) (*models.Platform, error)
	CreatePlatform(p *models.Platform) error
	UpdatePlatform(p *models.Platform) error
	DeletePlatform(id int64) error
	DeletePlatformCascade(id int64) error
	SetPlatformEnabled(id int64, enabled bool) error
	SetPlatformSortOrder(ids []int64) error
	UpdatePlatformFormats(platformID int64, formatsJSON string, propagateToRAPIs bool, formatEndpointsJSON string) error

	// ---- platform_keys ----
	GetPlatformKeys(platformID int64) ([]models.PlatformKey, error)
	GetAllPlatformKeys() ([]models.PlatformKey, error)
	SetPlatformKeys(platformID int64, keys []models.PlatformKey) error
	AddPlatformKey(k *models.PlatformKey) error
	UpdatePlatformKey(k *models.PlatformKey) error
	DeletePlatformKey(keyID int64) error
	DetachKeyFromRAPIs(keyID int64) ([]string, error)

	// ---- rapi ----
	GetRAPIs() ([]models.RAPIWithPlatform, error)
	GetRAPIByID(id int64) (*models.RAPIWithPlatform, error)
	GetRAPIsByPlatform(platformID int64) ([]models.RAPIWithPlatform, error)
	CreateRAPI(r *models.RAPI) error
	UpdateRAPI(r *models.RAPI) error
	DeleteRAPI(id int64) error
	DeleteRAPICascade(id int64) error
	SetRAPIEnabled(id int64, enabled bool) error
	SetRAPISortOrder(ids []int64) error
	UpdateRAPIFormats(id int64, formatsJSON string) error
	UpdateRAPIHeaders(id int64, headersJSON string) error
	RAPIAliasExists(platformID int64, alias string, excludeID int64) (bool, error)

	// ---- lapi / 路由链 ----
	GetLAPIs() ([]models.LAPI, error)
	GetLAPIByID(id int64) (*models.LAPI, error)
	GetLAPIByAlias(alias string) (*models.LAPI, error)
	FindLAPIByModelIdentity(vendor, series, version, suffix string) (*models.LAPI, error)
	CreateLAPI(u *models.LAPI) error
	UpdateLAPI(l *models.LAPI) error
	DeleteLAPI(id int64) error
	SetLAPIEnabled(id int64, enabled bool) error
	GetRAPIsForLAPI(lapiID int64) ([]models.RAPIWithPlatform, error)
	GetLAPIRAPIMapping(lapiID int64) ([]int64, error)
	SetLAPIRAPIOrder(lapiID int64, rapiIDs []int64) error
	GetRAPIsForLAPIWithStats(lapiID int64) ([]db.RAPIStat, error)
}

// *db.DB 满足接口（本地实现零包装、零行为变化）。
var _ Store = (*db.DB)(nil)

var (
	mu     sync.RWMutex
	active Store
)

// Init 在 service.Run 里 db.Init 之后调用，绑定本地实现。
func Init(local *db.DB) {
	mu.Lock()
	active = local
	mu.Unlock()
}

// Use 切换激活实现（管理模式换成 supabase Store；测试注入用）。
func Use(s Store) {
	mu.Lock()
	active = s
	mu.Unlock()
}

// A 返回当前激活的 Store。未 Init（如部分测试场景）时兜底直连本地 db.Get()。
func A() Store {
	mu.RLock()
	s := active
	mu.RUnlock()
	if s != nil {
		return s
	}
	return db.Get()
}
