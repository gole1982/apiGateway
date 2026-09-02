# 中心 / 端配置同步（在线更新）设计

- 日期：2026-09-03
- 状态：草案（待评审）
- 范围：新增 `cmd/center`（或 `service.role=center|edge`）、`internal/db`（端侧）、`internal/service`（bundle API）、`internal/crypto`（age 信封）、`internal/service/dashboard.html`（中心管理 UI / 端只读视图 + 手动更新）、`internal/fsm` + `internal/scheduler`（状态重置）

## 1. 背景与目标

当前 1 套网关、3 台机器各跑一个实例，配置（平台 / key / 模型 / 路由）需三处重复录入。目标是**在线更新**：把"定义类配置"收敛到一个中心权威源，端侧启动时主动拉取 1 次、并支持手动更新，之后端侧只做**拉取 + 流量转发**。

明确**不采用**"整体迁移到在线共享数据库"：热路径每请求同步 UPSERT `rapi_metrics`/`request_trends` + 异步写 `request_logs`（见 [db.go:54](L54) `SetMaxOpenConns(1)` + 全局 `db.mu` 串行化、[storage.go:117](L117) 每请求大字段 `INSERT OR REPLACE`），换远程库会把网络 RTT 塞进关键路径，且被动健康态跨机串扰。中心 / 端分离正好规避这四点。

## 2. 权限模型（中心权威 vs 端权威）

| 数据 | 归属 | 更新时的处理 |
|------|------|--------------|
| **定义类**：platform(name, base_url, supported_formats, format_endpoints, custom_headers, billing_address, login_account)、platform_keys(token, label, key_index, expires_at, is_free)、rapi(alias, model, vendor/series/model_name/version/suffix, notes, base_cost, high_cost, *_limit, time_period_rules, supported_formats, custom_headers, key_ids, source)、lapi、lapi_rapi_order、tools | **中心 PG 权威** | 每次更新全量覆盖到中心值 |
| **启用/禁用**：`platform.enabled`、`rapi.enabled`、`lapi.enabled`、`platform_keys.enabled` | **中心 PG 权威（例外）** | 覆盖到中心值；端侧不再提供 enabled 开关 |
| **健康/状态类**：`available`、`unavailable_reason`、`failure_type`、`failure_reason`、`failed_at`、`key_model_blocks`、调度器内存冷却 | **端权威** | 中心不下发；更新时**显式重置为初始值**（见 §5） |
| **遥测**：`rapi_metrics`、`request_trends`、`request_logs`、`sessions`、`log_events`、analytics | **端本地** | 不参与同步（默认保留，见 §5 说明） |
| **密钥根**：`~/.apiGateway.key`（AES-GCM 本地存储加密） | **端本地随机** | 不参与同步；age 私钥另存（见 §4） |

要点：`enabled`（用户意图）与 `available`（系统观察）在现有 schema 本就是两列，天然对应"中心权威 / 端权威"的切分，无需新增列。

## 3. 中心侧前端：Aiven 不提供托管

**结论：Aiven 没有 Cloudflare Pages 那样的前端 / 静态站点托管。** Aiven 是"托管开源数据基础设施"（PostgreSQL / MySQL / ClickHouse / Kafka / OpenSearch / Redis(Valkey) / Grafana 等），产品线只有数据服务 + 连接器，没有 Web/边缘前端托管（见其定价与文档：每个入口都是某个数据库产品）。因此 Aiven **只当中心 PG 用**；管理 UI + bundle API 必须另找宿主。

推荐宿主（任选其一，均能直连 Aiven PG over TLS）：

- **同一个 Go 二进制跑 `role=center`**，dashboard 绑 `0.0.0.0` + 前置 TLS（最省代码，复用现有 `internal/service` CRUD 与 `dashboard.html`），部署在 Fly.io / Railway / Render / 一台小 VPS。
- 静态管理 UI 放 Cloudflare Pages / GitHub Pages，API 放上面任一宿主（前后端分离）。

架构取舍：**一份代码、`role: center|edge` 双形态**最省心。
- 中心：PG 持久化（**仅需覆盖定义表的低频 CRUD**，中心不转发流量 → 不需要把 metrics/trends/logs/FSM 也搬上 PG，方言改造范围极小，全新建表、无 SQLite 迁移包袱）；暴露管理 UI（新增/编辑/模型发现/建路由/工具管理全挪到这里）；暴露 `GET /api/bundle`。
- 端：SQLite 现状不动（热路径 + 本地状态），隐藏"新增"UI、enabled 只读，新增"手动更新"，负责转发流量。

## 4. Bundle 接口与密钥信封

### 4.1 接口（HTTP + JSON，不用 gRPC）

```
GET /api/bundle?since=<version>   → 200 { version, generated_at, bundle: {...} }
                                  → 304 (version 未变，短路)
```

`bundle` 为全量定义快照：`platforms[]`、`platform_keys[]`（token 字段为密文）、`rapis[]`、`lapis[]`（含 `rapi_refs`：按业务键表达的路由链顺序）、`tools[]`。用**业务键**引用（`platform.name`、`platform_id+alias`、`lapi.alias`），不用自增 id，避免两端 id 错位。

### 4.2 密钥字段：age 信封加密（非对称、无 CA）

- 每台机器首启 `age`（X25519）生成密钥对；私钥存本机（与 `~/.apiGateway.key` 同级），公钥登记到中心（设备注册）。
- token 字段格式：`age:<recipient-epk>:<b64-ciphertext>`；中心持密文、无私钥，**零知识中转**。其余字段明文（可读、可 diff）。
- 端侧 import：age 私钥解出明文 token → 立即用本机 `~/.apiGateway.key` AES-GCM **重新加密入库**（与任意本地录入 token 同一条路）。非对称只在传输边界用一次，运行时热路径零改动。

**为什么不需要 CA/RA**：CA/RA 解决"公钥归属"的信任与签发/吊销。3 台同一人、互信的设备用更轻的方式即可——
- 传输信任：HTTPS（Aiven/宿主自带 TLS，服务端证书已由 CA 体系背书）+ 一个**预共享 sync 口令**做 `Authorization`；
- 消息信任：设备注册时人工核对一次 age 公钥指纹（TOFU），中心维护一张静态公钥清单；
- 吊销：手改清单即可，不必 CRL/OCSP。
仅当设备规模化 / 多租户 / 需自动吊销时，再引入自建小根 CA。

## 5. 端侧应用：全量更新 + 状态重置 + 版本兜底

### 5.1 触发时机

- 端启动后主动拉取 **1 次**（不轮询）；
- 控制台"手动更新"按钮。

### 5.2 应用语义（推荐 upsert-by-业务键，而非 delete+reinsert）

"全部更新" = **每一个同步字段都覆盖成中心值**，中心没有的行本地删除。实现按业务键 diff-upsert，**不用** `DELETE FROM rapi` 再插——因为 `rapi_metrics`/`lapi_rapi_order`/`token_cache` 对 rapi/lapi 是 `ON DELETE CASCADE`（[db.go:140-168](L140-L168)），盲目删表会顺带清空统计与路由映射，且 id 重排。upsert 既满足"全量覆盖定义+enabled"，又能精确控制"只重置健康态、保留计数/日志"。

### 5.3 状态重置（防 FSM 出错）

更新事务内，对**健康/状态类列显式归零**：`platform.available=1`、`rapi.available=1 & unavailable_reason=''`、`platform_keys.failure_type=0/reason=''/failed_at=NULL`、清空 `key_model_blocks`、并**重建 scheduler 内存快照**（`internal/scheduler/snapshot.go`）。原因：定义行变了，FSM（`internal/fsm/fsm.go`）若读到挂在新行上的旧观察态会误判；重置后 FSM 从干净态重新探测收敛。这与既有 `DisableExpiredKeys` 启动即洗一遍的模式一致。

> 待确认：`rapi_metrics`/`request_trends` 计数与 `request_logs` 日志默认**保留**（不受重置影响，FSM 不消费它们）。若你希望"更新即连历史统计一起清零"，把 §5.2 对这两张表也做显式清空即可——请给一句定夺。

### 5.4 数据版本兜底（fail-open）

- 端侧新增轻量表 `sync_state(id=1, current_version, last_good_version, last_synced_at, source_url)`；
- 应用前先校验 bundle 完整 + schema 合法，**通过后才在单事务里切换**并写 `last_good_version`；
- 拉取失败 / 中心不可达 / bundle 明显异常（如空定义集，防误删一切）→ 保留 `current_version` 对应的本地数据继续服务，绝不降级/清空；
- "手动更新"失败给出显式报错，不动现有配置。

## 6. 改动清单

**中心（新增/改造）**
1. `db` 定义表 Postgres 适配：全新 schema（`SERIAL/BIGSERIAL`、`BOOLEAN`、`TIMESTAMPTZ`、`JSONB`、`INSERT ... ON CONFLICT ... DO UPDATE`、`RETURNING id` 取代 `LastInsertId`）；仅需 CRUD + 模型发现，不迁 metrics/logs。
2. `GET /api/bundle` handler（读定义表 → 组装业务键 JSON → token 字段 age 加密 → 附 version）+ 预共享口令鉴权。
3. dashboard 管理 UI 绑 `0.0.0.0`+TLS；`/api/bundle` 版本来源（内容哈希或自增 `config_version`）。
4. 设备注册接口（存 age 公钥清单）+ 注册口令。

**端（改造）**
5. 启动 pull-once + "手动更新"按钮 → `applyBundle()`（§5.2/5.3 事务）。
6. `crypto`：age 信封加/解 + 本机公钥生成 / 私钥落盘。
7. dashboard：隐藏"新增/编辑/enabled 开关"，定义只读 + 保留本地状态展示（available/failure）；暴露"手动更新"和当前 `current_version`。
8. `sync_state` 表 + fail-open 逻辑（§5.4）。

## 7. 分步实施

1. 定 schema：定义/状态字段归属清单最终确认（含 §5.3 metrics 取舍）。
2. 中心 PG 定义层 CRUD + `/api/bundle`（先不含加密，跑通结构）。
3. age 信封：设备注册 + token 字段加解密 + 端侧本地重加密入库。
4. 端侧 `applyBundle`（upsert + 状态重置 + 事务 + fail-open + `sync_state`）。
5. dashboard 中心管理态 / 端只读态双形态（`role` 驱动）。
6. 联调 3 机：启动拉取、手动更新、断网兜底、FSM 重收敛回归。

## 8. 风险

- 定义表 PG 改造是主要工作量，但被"中心不转发流量、只 CRUD 定义表"大幅缩小（这是本方案相对"整体迁移"的核心收益）。
- bundle 全量覆盖属破坏性操作：靠 §5.4 的"先验后切 + 保留 last-good + 空集保护"兜底。
- 密钥过网面：token 密文传输 + 预共享口令 + TLS；明文只在端侧内存与本地密文库中出现。
