# 中心 / 管理 / 代理 配置同步（Supabase 轮询模型）设计

- 日期：2026-09-03（2026-09-08 重定为简化 3 组件模型）
- 状态：设计定稿（待批准后实现）
- 变更：由"Git 仓库为权威中心 + PR 闸门"重定为 **Supabase 中心 + 单写者管理端直写 + 代理轮询版本号**。动机：Git/PR 模型有非零合并延迟，多端并发编辑会产生重名 / 孤儿 / 删除竞态；单写者 + 写穿透 + 版本轮询把并发控制与广播机制整体绕开。原 Realtime / RLS / age 信封 / 两套件打包降级为 §9 可选硬化项。
- 范围：中心 Supabase（定义表 + 版本号 + `get_bundle` RPC）、`internal/bundle`（Pull / Validate / Apply + `sync_state`）、`internal/service` + `dashboard.html`（管理端直写 Supabase / 代理端只读 + 轮询）、`cmd/gateway`（启动拉取 + 定时轮询 + fail-open）。

## 1. 背景与目标

1 套网关、3 台机器各跑一个实例，配置（平台 / key / 模型 / 路由）需三处重复录入。目标：**在线更新**——把"定义类配置"收敛到一个中心权威源，管理端实时直写中心，代理定时拉取更新后只做**流量转发**。

明确**不采用**"整体迁到在线共享数据库 + 热路径直读"：热路径每请求同步 UPSERT `rapi_metrics`/`request_trends` + 异步大字段写 `request_logs`（[db.go:54](L54) `SetMaxOpenConns(1)` + 全局 `db.mu` 串行化、[storage.go:117](L117)），换远程库会把 RTT 塞进关键路径，且被动健康态跨机串扰。故**只有定义类配置进中心**，健康态 / 遥测留代理本地。

中心承载最终选定 **Supabase Postgres**：托管免费、自带 PostgREST 自动 REST、零自建后端；单写者约定下不需要 Realtime / RLS / Auth，中心最轻。

## 2. 权威模型（中心权威 vs 代理权威）

| 数据 | 归属 | 更新时的处理 |
|------|------|--------------|
| **定义类**：platform(name, base_url, supported_formats, format_endpoints, custom_headers, billing_address, login_account)、platform_keys(label, key_index, expires_at, is_free；token 见 §4 加密)、rapi(alias, model, vendor/series/model_name/version/suffix, notes, base_cost, high_cost, *_limit, time_period_rules, supported_formats, custom_headers, key_ids, source)、lapi、lapi_rapi_order、tools | **中心（Supabase）权威** | 管理端直写中心；代理全量覆盖到中心值 |
| **启用/禁用**：`platform.enabled`、`rapi.enabled`、`lapi.enabled`、`platform_keys.enabled` | **中心权威** | 覆盖到中心值；代理端不再提供 enabled 开关 |
| **健康/状态类**：`available`、`unavailable_reason`、`failure_type`、`failure_reason`、`failed_at`、`key_model_blocks`、调度器内存冷却 | **代理本地权威** | 中心不下发；代理拉新版本时**显式重置**（§5.3） |
| **遥测**：`rapi_metrics`、`request_trends`、`request_logs`、`sessions`、`log_events`、analytics | **代理本地** | 不同步、不清零 |
| **本地存储密钥根**：`~/.apiGateway.key`（AES-GCM 本地存储加密） | **代理本地随机** | 不同步；中心密钥另配（§4.3） |

`enabled`（用户意图）与 `available`（系统观察）在现有 schema 本就是两列，天然对应"中心权威 / 代理权威"的切分，无需新增列。

## 3. 三组件

### 3.1 中心（Supabase）

- Postgres 定义表（镜像现有 SQLite 定义表列子集，去 runtime 列 `available` / `unavailable_reason` / `failure_type` / `failure_reason` / `failed_at` / `key_model_blocks`）。
- `config_meta(version BIGINT, updated_at)`，**每次定义表的增删改在事务 COMMIT 时把 version +1**（commit 级触发器，非逐行——避免代理在一个逻辑 CRUD 中间拉到半应用态）。
- `get_version()` RPC：只返回当前 `version`（一个 BIGINT）。代理轮询只调这个，便宜。
- `get_bundle(p_version BIGINT DEFAULT NULL)` RPC：返回 `{version, bundle:{platforms, platform_keys, rapis, lapis, lapi_rapi_order, tools}}`；`p_version` 等于当前 version 时返回空体（304 等价短路），代理只在 `get_version` 发现变了时才调这个拉全量。
- 单写者约定下**不启用 Realtime / RLS / Auth**（中心最轻）；后续要多管理并发再议（§9）。

### 3.2 管理（可选部署，运维者本机）

- 现有 dashboard（`internal/service` handler + `dashboard.html`）全功能：平台 / 密钥 / 模型 / 路由 CRUD、发现模型向导、格式自动检测。
- **定义类读/写直走 Supabase PostgREST**（同步、写穿透、失败即报错不静默）——所有 CRUD 立即反映到中心。
- 格式检测（`apiformat.DetectFormats`）与模型发现（拉上游 `/v1/models`）在管理端本地 Go 跑，结果写中心。
- **localhost 绑定**，不对外暴露；单一管理在线（操作纪律，§10）。

### 3.3 代理（默认部署，多数机器）

- `cmd/gateway`：启动 Pull 一次 → 定时轮询中心 version → 变了就全量重拉 `get_bundle` → 单事务 `Apply` 到本地 SQLite → 转发。
- 中心 / 网络不可达 → 沿用本地 last_good 缓存继续转发（**fail-open**）。
- 面板只读：隐藏新增 / 编辑 / enabled，保留本地健康 / 指标 / 日志展示 + "手动刷新"按钮 + `current_version`。

## 4. Bundle 契约、token 加密与导入正确性

### 4.1 契约：结构化

- Bundle 用一份共享 Go `bundle` 结构体（对齐 `internal/models`）双向 `json.Marshal/Unmarshal`，设 `SchemaVersion`；代理解析 `DisallowUnknownFields` + `Validate()`。**类型漂移在编解码当场暴露**，SQLite 列类型（`enabled`=0/1、`*_formats`=JSON-as-TEXT、时间格式）由同一结构体保证一致。
- **引用一律业务键**（`platform.name`、`platform_id+alias`、`lapi.alias`），不用自增 id；代理 apply 时解析成本地 id。

### 4.2 导入正确性

1. **代理应用前校验**：拉下来完整验一遍（每条 rapi 的平台存在、`lapi_rapi_order` 指向存在 rapi、`(platform, alias)` 不重、`key_ids` 不悬空、`SchemaVersion` 认识），全过才进单事务；任一失败整体 abort、保留 last_good（§5.4）。
2. **中心侧约束**（替代 git CI 闸）：定义表 `UNIQUE(platform.name)`、`UNIQUE(platform_id, alias)`、`FK ON DELETE CASCADE`——管理端写入时由 Postgres 当场拦重名 / 孤儿。这是多端并发场景的根治点（即便单写者约定被偶然违反也兜得住重名与引用孤儿）。
3. **golden-file 回归**：仓库存 `bundle.json` fixture + 期望 SQLite 结果，离线跑 `applyBundle()` 回归；因文件确定性，这比对活库测还稳。

### 4.3 token 加密

- 中心库的 token 字段**只存密文**。简化方案：一个**中心 AES-256 密钥**（`[sync].center_key`，管理端与代理端各配一份）；管理端写入前用它加密，代理端拉取后用它解出明文 → **立即用本机 `~/.apiGateway.key` 重新 AES-GCM 加密入库**（与本地录入同路），热路径零改动。
- 硬化可选（§9）：若不想中心密钥散落所有代理，改回 age 信封（按已注册设备公钥封装，未登记设备解不开），代价是多一套密钥分发。

## 5. 代理应用：全量更新 + 状态重置 + 版本兜底

### 5.1 触发

启动拉 1 次 + **定时轮询中心 version**（间隔 `[sync].poll_interval_sec`，默认 60s）+ 控制台"手动刷新"。version 不变短路、不拉全量。

### 5.2 应用语义：业务键 diff-upsert（非 delete+reinsert）

"全部更新" = 每个同步字段覆盖成中心值、中心没有的行本地删除。**禁止** `DELETE FROM rapi` 再插——`rapi_metrics`/`lapi_rapi_order`/`token_cache` 对 rapi/lapi 是 `ON DELETE CASCADE`（[db.go:140-168](L140-L168)），盲删会清空统计、重排 id。按业务键 upsert 才能"全量覆盖定义 + enabled"且"只重置健康态"。

### 5.3 状态重置（防 FSM 出错）—— metrics 定稿保留

更新事务内**显式归零健康态**：`platform.available=1`、`rapi.available=1 & unavailable_reason=''`、`platform_keys.failure_type=0/reason=''/failed_at=NULL`、清 `key_model_blocks`、**重建 scheduler 快照**（`internal/scheduler/snapshot.go`），FSM（`internal/fsm/fsm.go`）从干净态重收敛。与既有 `DisableExpiredKeys` 启动即洗一致。
**定稿**：`rapi_metrics`/`request_trends`/`request_logs` **保留不清零**（FSM 不消费它们，重置无收益还丢历史）。

### 5.4 版本兜底（fail-open）

- 代理新增表 `sync_state(id=1, current_version, last_good_version, last_synced_at, source_url)`；
- §4.2 校验通过才单事务切换并写 `last_good_version`；
- 拉取失败 / 中心不可达 / bundle 异常（如空定义集，防误删一切）→ 沿用 last_good 继续服务，绝不降级 / 清空；"手动刷新"失败显式报错、不动现有配置。

## 6. 录入 / 发现流程（管理端）

- 管理端是**唯一写入口**：dashboard 的所有 CRUD 实时直写 Supabase，中心 version 随即在 COMMIT 时 bump，下个轮询周期各代理自动拉到新配置。
- 模型发现 / 格式探测（`apiformat.DetectFormats`、拉上游 `/v1/models`）作为**管理端本地动作**运行（复用 Go、零重写），结果写中心。
- **单一管理在线约定**：若两台管理同时上线且并发写，靠 §4.2 的中心 UNIQUE/FK 约束兜底重名 / 引用孤儿，但**删除竞态不保证**（A 删 p1 与 B 在 p1 下加 key 仍可能交叉）——故默认靠操作纪律只开一台管理；需强制时加 advisory lock（§10）。

## 7. 改动清单

**中心（新增 Supabase）**
1. 定义表（镜像 SQLite 定义表列子集）+ `config_meta.version` + commit 级 bump 触发器 + `get_bundle(version)` RPC。

**代理（改造 `cmd/gateway`）**
2. `internal/bundle`：契约结构体（对齐 `models`）+ `Validate()` + `Apply()`（§5.2 upsert + §5.3 重置 + 事务）。
3. Supabase RPC 拉取客户端（version 短路）+ `sync_state` 表 + fail-open；启动拉 1 次 + 定时轮询 + "手动刷新"。
4. dashboard 代理态：隐藏"新增 / 编辑 / enabled"，定义只读 + 保留本地状态展示；暴露"手动刷新"与 `current_version`。
5. token 解密 + 本地重加密入库（§4.3）。

**管理（改造 `internal/service`）**
6. dashboard 定义类读 / 写改走 PostgREST（`Store` 接口 + `supabaseStore` 实现）；runtime 读（指标 / 日志）仍走本地 SQLite。
7. token 写入前用中心密钥加密。
8. localhost 绑定 + 单一在线约定。

## 8. 分步实施

0. 本设计文档过审（当前阶段）。
1. 中心：Supabase 项目 + 定义表 schema + `config_meta.version` + commit 级 bump 触发器 + `get_bundle` RPC。
2. `internal/bundle`：结构体 + Validate + Apply（upsert + 状态重置 + 事务）+ `sync_state` + fail-open + fixture 回归。
3. 代理：`cmd/gateway` 启动 Pull + 定时轮询 version + 变更全量重拉 Apply + fail-open + 只读面板。
4. 管理：dashboard CRUD 改 PostgREST 直写 + `Store` 接口 + 检测 / 发现本地跑写中心 + localhost。
5. `proxy.cfg` 增 `[sync]` / `[management]`；E2E：管理 CRUD → version bump → 代理下轮拉到新配置；中心挂 → 代理 fail-open。

## 9. 备选 / 硬化

- **中心供应商**：Supabase 免费档首选（零运维）；若想更可控可自托管小服务（定义表 + version + bundle HTTP），但免费档够用。
- **多管理并发**：当前单写者约定。若将来要多管理并发，启用 Supabase Realtime（`postgres_changes` 广播）+ RLS（按身份行级隔离）+ age 信封密钥——即本轮简化前的方案，作为硬化升级路径。
- **token 加密**：当前中心 AES 密钥（简）；硬化改 age 信封。

## 10. 风险

- **单管理在线靠操作纪律**：两台管理并发写时，中心 UNIQUE/FK 兜重名 / 引用孤儿，但删除竞态不保证。需强制时加 advisory lock（几行，不破坏设计）。
- **"及时更新" = 轮询间隔延迟**：配置类 60s 够；唯一会吃延迟的场景是"作废被泄露的 key 要立刻生效"——那种才需要 push，目前轮询是正确取舍。
- **版本号 bump 粒度**：必须 commit 级（非逐行），否则代理可能拉到半应用态；或靠 MVCC 快照读 + 下轮自愈。
- **代理本地刷新原子性**：单事务 swap + 失败保留 last_good + fail-open，绝不半刷新。
- `Apply()` 是正确性集中点（任何拉取方案都躲不开）→ 靠 §4.2 校验 + 事务化全有或全无 + fixture 回归收敛。
- 中心 / 网络短时不可达 → fail-open 兜底，不中断转发；Supabase 免费档 7 天不活跃暂停，活跃使用不触发，暂停期代理继续用 last_good 转发。
