# 中心 / 端配置同步（Git 模型 · 在线更新）设计

- 日期：2026-09-03
- 状态：设计定稿（待批准后实现）
- 变更：托管形态由早期"REST + 中心 PG"改定为 **Git 仓库为权威中心**；§5.3 metrics 定稿为**保留**；新增 §4 Bundle 契约与导入正确性。Aiven/Supabase/Worker 降为 §9 备选。
- 范围：新增 `cmd/publish`（Go 发现回写成 PR）、`internal/bundle`（定型契约 + 校验 + apply）、`internal/db`（`sync_state` 表）、`internal/crypto`（age 信封）、`internal/service` + `dashboard.html`（端只读视图 + 手动更新）、`internal/fsm` + `internal/scheduler`（状态重置）、新仓库 + CI。

## 1. 背景与目标

1 套网关、3 台机器各跑一个实例，配置（平台 / key / 模型 / 路由）需三处重复录入。目标：**在线更新**——把"定义类配置"收敛到一个中心权威源，端启动时拉取 1 次 + 支持手动更新，之后端只做**拉取 + 流量转发**。

明确**不采用**"整体迁到在线共享数据库"：热路径每请求同步 UPSERT `rapi_metrics`/`request_trends` + 异步大字段写 `request_logs`（[db.go:54](L54) `SetMaxOpenConns(1)`+全局 `db.mu` 串行化、[storage.go:117](L117)），换远程库会把 RTT 塞进关键路径，且被动健康态跨机串扰。

中心承载最终选定 **Git**：写=合并（强闸门）、版本/回滚/审计内建、全量拉取天然匹配、零自建后端、永久免费。

## 2. 权限模型（中心权威 vs 端权威）

| 数据 | 归属 | 更新时的处理 |
|------|------|--------------|
| **定义类**：platform(name, base_url, supported_formats, format_endpoints, custom_headers, billing_address, login_account)、platform_keys(label, key_index, expires_at, is_free；token 见 §4 密文)、rapi(alias, model, vendor/series/model_name/version/suffix, notes, base_cost, high_cost, *_limit, time_period_rules, supported_formats, custom_headers, key_ids, source)、lapi、lapi_rapi_order、tools | **中心（git main）权威** | 全量覆盖到中心值 |
| **启用/禁用**：`platform.enabled`、`rapi.enabled`、`lapi.enabled`、`platform_keys.enabled` | **中心权威（例外）** | 覆盖到中心值；端不再提供 enabled 开关 |
| **健康/状态类**：`available`、`unavailable_reason`、`failure_type`、`failure_reason`、`failed_at`、`key_model_blocks`、调度器内存冷却 | **端权威** | 中心不下发；更新时**显式重置**（§5.3） |
| **遥测**：`rapi_metrics`、`request_trends`、`request_logs`、`sessions`、`log_events`、analytics | **端本地** | 不同步、**不清零**（§5.3 定稿） |
| **密钥根**：`~/.apiGateway.key`（AES-GCM 本地存储加密） | **端本地随机** | 不同步；age 私钥另存（§4） |

`enabled`（用户意图）与 `available`（系统观察）在现有 schema 本就是两列，天然对应"中心权威 / 端权威"的切分，无需新增列。

## 3. 中心 = Git 仓库

- 私有仓 `apigw-config`。`main` = 权威快照，开**分支保护**：禁止直接 push、必须 PR + 审查、必需状态检查（CI）通过。→ 满足"中心难以被改"：改它 = 你得把 PR 合进去。
- 布局（推荐"多源文件 + CI 产物"）：
  - `config/{platforms,rapis,lapis,tools}/*.json` —— 人编辑、diff 友好；
  - `bundle/schema_version` —— 契约版本；
  - **CI（GitHub Action）** 从源文件 `models` 结构体编译出单一 `dist/bundle.json` + 计算 `version=<commit-sha>` + 跑 §4 校验，打 Release 产物。端只拉这个**已校验产物**。
- 免费栈：私有仓 + Actions（免费额度足够个人）+ fine-grained PAT(`contents:read`) + Releases。管理表单用 GitHub 原生 Web UI 即可；Cloudflare Pages 仅在你想要更友好表单时再配，**非必需**。

## 4. Bundle 契约、密钥与导入正确性

### 4.1 契约：结构化，不是"随手文件"

- Bundle 用**一份共享 Go `bundle` 结构体**（对齐 `internal/models`）双向 `json.Marshal/Unmarshal` 生成/解析，设 `SchemaVersion`；端解析 `DisallowUnknownFields` + `Validate()`。**类型漂移在编解码当场暴露**，SQLite 列类型（`enabled`=0/1、`*_formats`=JSON-as-TEXT、时间格式）由同一结构体保证一致——这正是消解"git 存非结构化 → 导入错"的关键。
- **引用一律业务键**（`platform.name`、`platform_id+alias`、`lapi.alias`），不用自增 id；端 apply 时解析成本地 id。

### 4.2 导入正确性三道闸（替代 Postgres 静止态约束）

1. **CI 合并前校验**：每个 `rapi` 的平台存在、每条 `lapi_rapi_order` 指向存在 rapi、`(platform, alias)` 不重、`key_ids` 不悬空、`SchemaVersion` 认识。不过不许合。
2. **端应用前校验**：拉下来完整验一遍，**全过才进单事务**；任何一条失败整体 abort、保留 last-good（§5.4）。绝不做"半推半就"式写入。
3. **golden-file 回归**：仓库存 `bundle.json` fixture + 期望 SQLite 结果，离线跑 `applyBundle()` 回归；因文件确定性，这比对活库测还稳。

### 4.3 密钥：age 信封（非对称、无 CA）

- 每台机器首启生成 `age`(X25519) 密钥对；私钥本机存，公钥登记进 `config/devices`（注册即身份）。
- token 字段 `age:<recipient-epk>:<b64-ciphertext>`，**按全体已登记设备公钥**封装 → 未登记设备连解都解不开（读鉴权密码学强制）。中心持密文、无私钥，零知识。
- 端 import：age 私钥解出明文 token → **立即用本机 `~/.apiGateway.key` 重新 AES-GCM 加密入库**（与本地录入同路）。非对称只在边界用一次，热路径零改动。
- **红线**：git 里**只进 age 密文，永不进明文密钥**（历史近乎不可变）；开 GitHub Secret Scanning / Push Protection 兜底。
- 无需 CA/RA：写=受保护 PR + 审查（GitHub 账号即信任根），读=只读令牌；3 台互信设备规模下 CA/CRL 属过度设计。

## 5. 端应用：全量更新 + 状态重置 + 版本兜底

### 5.1 触发

端启动主动拉 **1 次**（不轮询）+ 控制台"手动更新"。走 GitHub API/Release 取 `dist/bundle.json`（带 `If-None-Match`/sha 短路）。

### 5.2 应用语义：业务键 diff-upsert（非 delete+reinsert）

"全部更新" = 每个同步字段覆盖成中心值、中心没有的行本地删除。**禁止** `DELETE FROM rapi` 再插——`rapi_metrics`/`lapi_rapi_order`/`token_cache` 对 rapi/lapi 是 `ON DELETE CASCADE`（[db.go:140-168](L140-L168)），盲删会清空统计、重排 id。按业务键 upsert 才能"全量覆盖定义+enabled"且"只重置健康态"。

### 5.3 状态重置（防 FSM 出错）—— metrics 定稿保留

更新事务内**显式归零健康态**：`platform.available=1`、`rapi.available=1 & unavailable_reason=''`、`platform_keys.failure_type=0/reason=''/failed_at=NULL`、清 `key_model_blocks`、**重建 scheduler 快照**（`internal/scheduler/snapshot.go`），FSM(`internal/fsm/fsm.go`) 从干净态重收敛。与既有 `DisableExpiredKeys` 启动即洗一致。
**定稿**：`rapi_metrics`/`request_trends`/`request_logs` **保留不清零**（FSM 不消费它们，重置无收益还丢历史）。

### 5.4 版本兜底（fail-open）

- 端新增表 `sync_state(id=1, current_sha, last_good_sha, last_synced_at, repo_ref)`；
- §4.2 校验通过才单事务切换并写 `last_good_sha`；
- 拉取失败 / GitHub 不可达 / bundle 异常（如空定义集，防误删一切）→ 沿用 last-good 继续服务，绝不降级/清空；"手动更新"失败显式报错、不动现有配置。

## 6. 发现 / 新增流程（保住功能，又不给端写权限）

核心：**常驻端进程永不持有写/发 PR 的令牌**。改中心只经"你 + 受保护 PR"。

- **手动新增/编辑**：GitHub Web UI（或 Pages 表单）→ 提 PR → 你合。
- **模型自动发现 / 格式探测**（`apiformat.DetectFormats` 等，**全部复用 Go、零重写**）：作为**管理态一次性动作**运行——
  - 落地 A（推荐）：`cmd/publish` 在本机用 Go 跑发现，要求操作者提供**短效作用域令牌**（仅此刻、跑完即回收），生成 age 密文改动 → **开 PR**。这是"你、认证过、一次性"，非 7×24 端能力。
  - 落地 B（更保守）：发现结果导成 JSON 文件（token 已本机 age 封好）→ 你在 UI 上传导入。连回写令牌都不给机器。
- 合并进 main → 下次端拉取即生效（三端统一，爆炸半径=三台，故保留 git 历史/revert + §5.4 双向兜底）。

## 7. 改动清单

**仓库 + CI（新增）**
1. `apigw-config` 私有仓 + `config/*.json` 布局 + `main` 分支保护。
2. Action：源→`dist/bundle.json` 编译 + `version=sha` + §4.2 校验 + 发 Release + secret scanning。

**端（改造）**
3. `internal/bundle`：契约结构体（对齐 `models`）+ `Validate()` + `applyBundle()`（§5.2 upsert + §5.3 重置 + 事务）。
4. `internal/crypto`：age 密钥对生成/私钥落盘/信封加解 + `~/.apiGateway.key` 重加密入库。
5. GitHub raw/Release 拉取客户端（sha 短路）+ `sync_state` 表 + fail-open（§5.4）；启动拉 1 次 + "手动更新"。
6. dashboard：隐藏"新增/编辑/enabled"，定义只读 + 保留本地状态展示（available/failure）；暴露"手动更新"与 `current_sha`。

**发布（新增）**
7. `cmd/publish`：Go 发现（复用 `DetectFormats`）+ age 封装 + 以短效令牌开 PR（或导出文件）。

## 8. 分步实施

1. 契约定稿：`bundle` 结构体 + `SchemaVersion` + 校验清单 + golden fixture（先冻结格式）。
2. 仓库 + CI：布局、分支保护、编译产物、校验闸。
3. 端 applyBundle：upsert + 状态重置 + 事务 + fail-open + `sync_state`；带 fixture 回归。
4. age 信封 + 设备注册 + `cmd/publish` 开 PR。
5. dashboard 只读态 + "手动更新"按钮。
6. 联调 3 机：启动拉取、手动更新、断网/GitHub 不可达兜底、FSM 重收敛回归、误改 revert 演练。

## 9. 备选（若将来不用 Git）

- **Aiven 仅当 PG**：Aiven 无前端/计算托管；需 Pages+一个 Cloudflare Worker(JS) 做读写 REST；发现无法复用 Go（Worker 是 TS，须重写 `DetectFormats`）。
- **Supabase**：Postgres + 自动 REST(PostgREST)+Auth+RLS，能替 Aiven 且免写 CRUD 后端；仍不跑 Go，发现要么 Edge Function 重写、要么 Go 端回写。
两者都比 Git 多一套鉴权/RLS 布线且丢"合并即写闸门"，故当前不选。

## 10. 风险

- `applyBundle()` 是正确性集中点（任何拉取方案都躲不开）→ 靠 §4.2 三道闸 + 事务化全有或全无 + fixture 回归收敛。
- 编辑颗粒变粗（PR 往返）、非实时——与"人驱动、启动+手动"节奏匹配，可接受。
- 令牌误配风险：发 PR/写令牌只出现在管理态/一次性动作，常驻端仅 `contents:read`。
