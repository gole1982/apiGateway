# 定义层自然键身份模型（平台 / 凭据 / 端点 / 接口）设计

- 日期：2026-09-23
- 状态：设计定稿（待批准后实现）
- 前置：2026-09-03-center-edge-config-sync-design.md（同步骨架、版本机制、fail-open 不变，本文只替换"业务键"定义与 lapi 的 pull 语义）
- 范围：`internal/db`（schema 迁移）、`internal/bundle`（契约字段）、`internal/db/bundle_apply.go`、`internal/supabase/store.go`、`scripts/supabase_schema.sql`、`internal/scheduler`（双层限额）、`internal/service`（lapi 合并 + 改名）。

## 1. 背景与动机

现行业务键全部挂在**可变标签**上：platform.name、platform_keys.key_index、rapi.alias、lapi_rapi_order 经别名引用。后果：

- **易重复**：同一上游用两个名字各登记一次（手动录入 vs 自动发现），系统无法识别是同一资源；
- **易误删**：改名 = 换业务键 = 同步 stale 删除 + 重建，rapi_metrics / token_cache 被 ON DELETE CASCADE 连带清掉；
- 跨实例对齐依赖"各端起名一致"的默契，无约束兜底。

定稿方向：**身份只用构成要素**（base_url / token / 远端 model 名），名字一律降级为显示标签。

### 1.1 决策定稿（2026-09-23 对话确认）

1. **运行时状态边界不变**：中心只持配置，健康态/遥测永不出本地（沿用前置设计 §2，无变更）。自然键是该原则的纯推论：改标签 → 自然键不变 → 同一行 upsert，行上本地运行时状态续存；改构成要素 → 新行，运行时状态自然从零开始。不需要任何"状态继承"机制。
2. **限额双层**：端点级（跟模型，覆盖千问/DashScope 这类账号级模型配额）+ 绑定级（跟 key，默认层，绝大多数平台每 key 一个配额桶）。调度时两个桶都过，任一耗尽即换下一绑定/节点；0=不限（沿用现有约定）。现有限额迁移时默认落绑定层。
3. **同 model 名、不同 base_url = 不同端点**（官方 vs 中转各自独立健康/配额，可作为链上 fallback），这是特性。
4. **接口（lapi）pull 合并语义**见 §5.2；同名链不可比时**中心版占原名、本地版自动改名 `name#local` 保留**（中心权威优先，本地配置不丢）。

## 2. 身份模型

| 对象 | 现身份 | 新身份（构成要素） | 原标签去向 |
|---|---|---|---|
| 平台 | name | **归一化 base_url** | name → 显示名，可随便改 |
| 凭据 | (platform, key_index) | **token_hash = sha256(token) hex 前 16** | key_index 废除；label → 显示名 |
| 端点（沿用 rapi 表，术语改称端点） | (platform_name, alias) | **(base_url, model)** | alias → 显示名，可随便改 |
| 绑定 | rapi.key_ids CSV | **endpoint × credential 关联表** | — |
| 接口（lapi） | alias | **公开模型名**（不变——它本就是客户端契约） | — |

- **base_url 归一化**：scheme/host 小写、去默认端口（80/443）、去尾部 `/`；路径大小写保留。不做 DNS/重定向归一：同服务不同域名视为不同平台，由人工合并。
- **token_hash** 仅作相等性判据：API key 高熵，离线爆破不可行，无需加盐；中心与 bundle 携带 hash 不扩大泄漏面（密文 token 字段照旧，中心只存密文的约束不变）。
- **调度单元不变**：运行时最小单位仍是 (凭据, 端点, 模型) 三元组，绑定表与其天然同构；key_model_blocks / token_cache 改挂绑定/凭据 id。

### 2.1 明确不采用

- **三元组整体做单行主键**：一端点多 key（多凭据轮询）会按 key 复制定义行，限额/成本/链配置全部重复维护。拆成端点 + 凭据 + 绑定后，去重语义各自干净，三元组保留为调度/健康单元而非定义主键。
- **平台继续用 name 作身份**：名字可改可撞，作身份必然重演重复与误删。

## 3. 目标表结构（本地 SQLite；中心镜像同构、去 runtime 列）

```sql
-- 平台：base_url 即身份。token/last_token_fetch/login_* 等平台级抓取流字段保留。
CREATE TABLE platform (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  base_url TEXT NOT NULL UNIQUE,          -- 归一化后，身份
  name TEXT NOT NULL DEFAULT '',          -- 显示名
  enabled INTEGER NOT NULL DEFAULT 1,
  token TEXT NOT NULL DEFAULT '', last_token_fetch DATETIME,
  supported_formats ..., format_endpoints ..., custom_headers ...,
  billing_address ..., login_account ..., login_password ...,
  sort_order ..., created_at ..., updated_at ...
  -- 本地另有 runtime 列：available（不进中心）
);

-- 凭据：token_hash 即身份，脱离平台
CREATE TABLE credential (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  token_hash TEXT NOT NULL UNIQUE,
  token TEXT NOT NULL,                    -- 本地 enc: 密文 / 中心 center_key 密文
  label TEXT NOT NULL DEFAULT '',
  enabled INTEGER NOT NULL DEFAULT 1,
  expires_at DATETIME, is_free INTEGER NOT NULL DEFAULT 0,
  created_at ..., updated_at ...
  -- 本地另有 runtime 列：failure_type/failure_reason/failed_at（不进中心）
);

-- 端点（沿用 rapi 表名）：(base_url, model) 即身份
CREATE TABLE rapi (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  base_url TEXT NOT NULL,                 -- 归一化；冗余自 platform 换取表级 UNIQUE
  model TEXT NOT NULL,
  alias TEXT NOT NULL DEFAULT '',         -- 显示名
  platform_id INTEGER NOT NULL DEFAULT 0, -- 降级为展示分组外键，不参与身份
  enabled ..., base_cost ..., high_cost ...,
  rpm_limit ..., rph_limit ..., rpd_limit ..., tpm_limit ..., tph_limit ..., tpd_limit ...,
  -- ^ 端点级限额（跟模型；千问/DashScope 模式），0=不限
  time_period_rules ..., supported_formats ..., custom_headers ...,
  vendor ..., series ..., model_name ..., version ..., suffix ..., notes ...,
  source ..., sort_order ..., created_at ..., updated_at ...,
  UNIQUE(base_url, model)
  -- 本地另有 runtime 列：available/unavailable_reason（不进中心）
);

-- 绑定：调度三元组的落库形态
CREATE TABLE endpoint_credential (
  endpoint_id INTEGER NOT NULL REFERENCES rapi(id) ON DELETE CASCADE,
  credential_id INTEGER NOT NULL REFERENCES credential(id) ON DELETE CASCADE,
  enabled INTEGER NOT NULL DEFAULT 1,
  rpm_limit ..., rph_limit ..., rpd_limit ..., tpm_limit ..., tph_limit ..., tpd_limit ...,
  -- ^ 绑定级限额（跟 key；默认层），0=不限
  UNIQUE(endpoint_id, credential_id)
);
```

- 健康态挂载点迁移：`key_model_blocks` → `(endpoint_id, credential_id)`；`token_cache` → `credential_id`。均本地，不进中心。
- `key_index` / `key_ids` CSV 彻底废除。

## 4. 接口（lapi）与链

- lapi 身份 = 公开模型名（客户端契约），UNIQUE 保留，不改。
- 链节点引用端点自然键：逻辑上 lapi_rapi_order 为 (lapi 公开名, base_url, model, order_index)；物理仍 (lapi_id, rapi_id) + 双 UNIQUE（`UNIQUE(lapi_id, rapi_id)`、`UNIQUE(lapi_id, order_index)`），bundle 传输换自然键。
- **链签名**（lapi 冗余列，链变更时在 service 层重算）：
  `canonical = 按 order_index 升序，normalize(base_url)#model 以 " > " 连接`；`signature = sha256(canonical) hex 前 16`。
  节点用自然键而非别名/id：改名不产生假差异，跨实例可直接比较。
- **链比较三级语义**（仅同名 lapi 之间单对单比较；不同名 = 不同客户端契约，永不互判）：
  - L0 相同（签名相等）→ 去重留一；
  - L1 同集异序（如 `1-2-3` vs `1-3-2`）→ 不同配置（fallback 优先级是语义本身），不可比，按冲突处理（§5.2）；
  - L2 子链（保持相对顺序的子序列，`1-3 ⊂ 1-2-3`）→ 留长删短；
  - L3 无关 → 各自独立。
  - 注：L2 用**子序列**不用子集，相对顺序不同即不可比。

## 5. 同步协议变更

### 5.1 bundle 业务键全换自然键

- platforms 以 base_url 引用；credentials 以 token_hash；rapis 以 (base_url, model)；lapi_rapi_order 节点为 (lapi 公开名, base_url, model, order_index)。
- rapi.key_ids CSV 消亡，绑定进 bundle：(endpoint 自然键, token_hash, 绑定限额, enabled)。
- 中心 schema 同步换 UNIQUE：`platform(base_url)`、`credential(token_hash)`、`rapi(base_url, model)`。scripts/supabase_schema.sql 原 key_ids TODO 注释随之作废（实际早已按 key_index 业务键传输，本次直接换 token_hash）。
- Validate / Apply 骨架不变，仅查找键替换；防误删护栏（空 bundle 拒绝、单事务、last_good）原样保留。

### 5.2 pull（中心→端）：lapi 特殊合并，其余表照旧

- platform / credential / rapi / endpoint_credential：照旧 diff-upsert + stale 删除（中心全权覆盖，中心没有本地就删）。
- **lapi：中心权威 + 本地保留区**。按公开名配对后比链：

  | 情形 | 动作 |
  |---|---|
  | 同名、链相同（L0） | 去重，留中心版 |
  | 同名、子链（L2） | 留长链一方：中心长 → 覆盖本地；本地长 → 保留本地 |
  | 同名、不可比（L1/L3） | 中心版占原名；本地版自动改名 `name#local` 保留（撞名递增 `name#local2`） |
  | 仅本地存在 | 保留，不下发也不删 |
  | 仅中心存在 | 下发新增 |

- `#local` 改名只发生在本地镜像，不回传中心；但后续显式 push（端→中心全覆盖，需 confirm）会把 `#local` 条目带上中心，是否清理由人工决定。
- lapi 因此成为唯一"中心权威但允许本地保留区"的表；其余定义表维持中心全权。

### 5.3 push（端→中心）

ReplaceAll 全覆盖，不变（含接口，无去重）。

## 6. 数据迁移（一次性，工具内置 dry-run：只出清单不落库）

1. **credential 回填**：platform_keys 行 → credential（算 token_hash）；同 hash 多行合并为一（label 取最长者），合并清单人工确认。
2. **rapi 回填 base_url**（从所属 platform 反带）；(base_url, model) 撞行合并：保留统计多者，绑定取并集，链引用批量改指幸存者，另一行删除，清单人工确认。
3. **key_ids CSV → endpoint_credential 行**；现有限额整体写入绑定层（保持现行"跟 key"语义）；千问类（vendor 为 aliyun/dashscope）提示改填端点层。
4. **平台 name → 显示名**，base_url 归一化后建 UNIQUE；归一化后撞 url 的平台列清单人工合并。

## 7. 分期

- **Phase 1**：credential 表 + endpoint_credential + rapi (base_url, model) 身份（本地先行；bundle 契约换键；中心 schema 换 UNIQUE）。
- **Phase 2**：双层限额（绑定层落地，调度双桶）。
- **Phase 3**：lapi pull 合并语义 + 链签名 + `#local` 改名。

## 8. 测试

- **golden-file 回归**：bundle fixture（含同名撞车、子链、不可比冲突、`#local` 撞名递增）→ apply 后期望 SQLite 快照，离线可跑。
- 迁移工具 dry-run 输出快照比对。
- 双层限额：调度器单测覆盖"端点桶耗尽换绑定、绑定桶耗尽换节点、两层 0=不限"。