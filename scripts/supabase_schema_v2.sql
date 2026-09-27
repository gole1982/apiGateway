-- ===========================================================================
-- apiGateway 中心库 schema —— 契约 v2（自然键身份模型）
-- 对应设计：docs/superpowers/specs/2026-09-23-natural-key-identity-design.md §5.1/§5.2
-- 契约源：internal/bundle/bundle.go（SchemaVersion = 2）
-- 导出源：internal/db/bundle_export.go（本地 SQLite 是唯一真源）
--
-- 用法：在 Supabase 项目的 SQL Editor 整体执行一次。
--
-- ⚠ 本脚本是**破坏性重建**，且已获运维明确授权：
--   · 中心定义数据全部清空，不保留任何旧行；
--   · 中心不"迁移"旧数据，而是随后由本地 ExportBundle 推送重建。
--
-- 身份键 v1 → v2：
--   platform      name                  → base_url        （name 降级为显示名）
--   credential    (platform, key_index) → token_hash      （全局唯一，内容判据）
--   rapi          (platform, alias)     → (platform, model)
--   lapi          alias（不变，仍是客户端公开契约，仍 UNIQUE）
--   端点↔凭据     rapi.key_ids CSV      → endpoint_credential 真实行
--
-- 关键约束：base_url 一律**原样**作键，禁止在 SQL 里归一化（小写化/去尾斜杠）。
-- 本地 SQLite 的唯一索引建在原始列上（idx_platform_base_url ON platform(base_url)），
-- Go 契约同样按原值比对；此处若归一化，两端会算出不同的键。
-- ===========================================================================


-- ---------------------------------------------------------------------------
-- 0. 先摘掉 v1 触发器（表要先 drop，触发器不能挂在已删表上）
--    platform_keys 用 DO 块守卫：Postgres 的 DROP TRIGGER IF EXISTS 在**表不存在**
--    时仍会报 "relation does not exist"，直接写会让脚本第二次执行失败（不可重入）。
-- ---------------------------------------------------------------------------
DROP TRIGGER IF EXISTS trg_bump_platform        ON platform;
DROP TRIGGER IF EXISTS trg_bump_rapi            ON rapi;
DROP TRIGGER IF EXISTS trg_bump_lapi            ON lapi;
DROP TRIGGER IF EXISTS trg_bump_lapi_rapi_order ON lapi_rapi_order;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = 'platform_keys') THEN
        EXECUTE 'DROP TRIGGER IF EXISTS trg_bump_platform_keys ON platform_keys';
    END IF;
END $$;

-- ---------------------------------------------------------------------------
-- 1. 【破坏性】删除 v1 专有对象
--    platform_keys 表整体废弃 → 由 credential 表取代（token_hash 身份）
--    rapi.key_ids 列废弃      → 由 endpoint_credential 绑定表取代
--    旧业务键唯一约束废弃      → name / (platform_id, alias) 不再是身份
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS platform_keys CASCADE;

ALTER TABLE rapi     DROP COLUMN IF EXISTS key_ids;
ALTER TABLE platform DROP CONSTRAINT IF EXISTS platform_name_key;         -- name 不再唯一
ALTER TABLE rapi     DROP CONSTRAINT IF EXISTS rapi_platform_id_alias_key;  -- alias 不再唯一

-- ---------------------------------------------------------------------------
-- 2. v2 定义表
--    runtime 列（available / unavailable_reason / failure_* / key_model_blocks）
--    仍不进中心：那是代理本地健康态（设计 §2 边界）。
-- ---------------------------------------------------------------------------

-- 2.1 platform：base_url 升为业务键（name 仅显示名）
CREATE TABLE IF NOT EXISTS platform (
    id                BIGSERIAL PRIMARY KEY,
    name              TEXT NOT NULL DEFAULT '',   -- 显示名（v2 起无唯一约束）
    base_url          TEXT NOT NULL DEFAULT '',   -- 业务键
    token             TEXT NOT NULL DEFAULT '',   -- center_key 密文
    last_token_fetch  TIMESTAMPTZ,
    enabled           BOOLEAN NOT NULL DEFAULT TRUE,
    notes             TEXT NOT NULL DEFAULT '',
    supported_formats TEXT NOT NULL DEFAULT '["openai"]', -- JSON array as TEXT
    format_endpoints  TEXT NOT NULL DEFAULT '',           -- JSON {format:url} as TEXT
    custom_headers    TEXT NOT NULL DEFAULT '',           -- JSON array as TEXT
    billing_address   TEXT NOT NULL DEFAULT '',
    login_account     TEXT NOT NULL DEFAULT '',
    login_password    TEXT NOT NULL DEFAULT '',           -- center_key 密文
    sort_order        INTEGER NOT NULL DEFAULT 0,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 2.2 credential：业务键 = token_hash（全局唯一，内容派生）
--     platform_id 保留：契约需要 platform_base_url（轮换按平台分组），
--     且 key 归属平台是真实语义，不是可推导的冗余。
--     sort_order = 平台内轮换序号（v1 的 key_index 降级为纯属性）。
CREATE TABLE IF NOT EXISTS credential (
    id          BIGSERIAL PRIMARY KEY,
    platform_id BIGINT NOT NULL REFERENCES platform(id) ON DELETE CASCADE,
    token_hash  TEXT,                                 -- 业务键；NULL = 不参与唯一
    sort_order  INTEGER NOT NULL DEFAULT 0,           -- 平台内轮换序号
    token       TEXT NOT NULL DEFAULT '',             -- center_key 密文
    label       TEXT NOT NULL DEFAULT '',
    enabled     BOOLEAN NOT NULL DEFAULT TRUE,
    expires_at  TIMESTAMPTZ,
    is_free     BOOLEAN NOT NULL DEFAULT FALSE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 2.3 rapi：业务键 = (platform.base_url, model)；alias 降级为显示名
CREATE TABLE IF NOT EXISTS rapi (
    id                BIGSERIAL PRIMARY KEY,
    platform_id       BIGINT NOT NULL REFERENCES platform(id) ON DELETE CASCADE,
    alias             TEXT NOT NULL DEFAULT '',  -- 显示名（v2 起无唯一约束）
    model             TEXT NOT NULL DEFAULT '',  -- 业务键的一半
    enabled           BOOLEAN NOT NULL DEFAULT TRUE,
    base_cost         INTEGER NOT NULL DEFAULT 0,
    high_cost         INTEGER NOT NULL DEFAULT 0,
    rpm_limit         INTEGER NOT NULL DEFAULT 0,
    rph_limit         INTEGER NOT NULL DEFAULT 0,
    rpd_limit         INTEGER NOT NULL DEFAULT 0,
    tpm_limit         INTEGER NOT NULL DEFAULT 0,
    tph_limit         INTEGER NOT NULL DEFAULT 0,
    tpd_limit         INTEGER NOT NULL DEFAULT 0,
    time_period_rules TEXT NOT NULL DEFAULT '',   -- JSON array as TEXT
    supported_formats TEXT NOT NULL DEFAULT '["openai"]',
    custom_headers    TEXT NOT NULL DEFAULT '',
    source            TEXT NOT NULL DEFAULT 'manual', -- auto_discover | manual
    vendor            TEXT NOT NULL DEFAULT '',
    series            TEXT NOT NULL DEFAULT '',
    model_name        TEXT NOT NULL DEFAULT '',
    version           TEXT NOT NULL DEFAULT '',
    suffix            TEXT NOT NULL DEFAULT '',
    notes             TEXT NOT NULL DEFAULT '',
    sort_order        INTEGER NOT NULL DEFAULT 0,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 2.4 endpoint_credential：端点↔凭据绑定，取代 v1 的 key_ids CSV
--     与本地表同名同构，便于 ExportBundle / ApplyBundle 逐字段对拍。
CREATE TABLE IF NOT EXISTS endpoint_credential (
    id            BIGSERIAL PRIMARY KEY,
    rapi_id       BIGINT NOT NULL REFERENCES rapi(id) ON DELETE CASCADE,
    credential_id BIGINT NOT NULL REFERENCES credential(id) ON DELETE CASCADE,
    rpm_limit     INTEGER NOT NULL DEFAULT 0,
    rph_limit     INTEGER NOT NULL DEFAULT 0,
    rpd_limit     INTEGER NOT NULL DEFAULT 0,
    tpm_limit     INTEGER NOT NULL DEFAULT 0,
    tph_limit     INTEGER NOT NULL DEFAULT 0,
    tpd_limit     INTEGER NOT NULL DEFAULT 0,
    enabled       BOOLEAN NOT NULL DEFAULT TRUE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(rapi_id, credential_id)
);

-- 2.5 lapi：alias 是客户端公开模型名，UNIQUE 保留，语义不变
CREATE TABLE IF NOT EXISTS lapi (
    id         BIGSERIAL PRIMARY KEY,
    alias      TEXT NOT NULL UNIQUE,
    notes      TEXT NOT NULL DEFAULT '',
    enabled    BOOLEAN NOT NULL DEFAULT TRUE,
    vendor     TEXT NOT NULL DEFAULT '',
    series     TEXT NOT NULL DEFAULT '',
    model_name TEXT NOT NULL DEFAULT '',
    version    TEXT NOT NULL DEFAULT '',
    suffix     TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 2.6 lapi_rapi_order：物理仍是 (lapi_id, rapi_id) + 双 UNIQUE，
--     但 bundle 传输换自然键 (lapi_alias, base_url, model) —— 见 get_bundle。
CREATE TABLE IF NOT EXISTS lapi_rapi_order (
    id          BIGSERIAL PRIMARY KEY,
    lapi_id     BIGINT NOT NULL REFERENCES lapi(id) ON DELETE CASCADE,
    rapi_id     BIGINT NOT NULL REFERENCES rapi(id) ON DELETE CASCADE,
    order_index INTEGER NOT NULL,
    UNIQUE(lapi_id, rapi_id),
    UNIQUE(lapi_id, order_index)
);

-- ---------------------------------------------------------------------------
-- 3. 业务键唯一索引（部分索引，与本地 SQLite 的 idx_* 完全对齐）
--    注意：全部建在**原始列**上，不做表达式归一化。
-- ---------------------------------------------------------------------------
CREATE UNIQUE INDEX IF NOT EXISTS idx_platform_base_url
    ON platform(base_url) WHERE base_url <> '';
CREATE UNIQUE INDEX IF NOT EXISTS idx_rapi_platform_model
    ON rapi(platform_id, model) WHERE model <> '';
CREATE UNIQUE INDEX IF NOT EXISTS idx_credential_token_hash
    ON credential(token_hash) WHERE token_hash IS NOT NULL;

-- 查询用辅助索引
CREATE INDEX IF NOT EXISTS idx_rapi_platform      ON rapi(platform_id);
CREATE INDEX IF NOT EXISTS idx_credential_platform ON credential(platform_id);
CREATE INDEX IF NOT EXISTS idx_lapi_rapi_lapi     ON lapi_rapi_order(lapi_id);
CREATE INDEX IF NOT EXISTS idx_endpoint_cred_rapi ON endpoint_credential(rapi_id);
CREATE INDEX IF NOT EXISTS idx_endpoint_cred_cred ON endpoint_credential(credential_id);

-- ---------------------------------------------------------------------------
-- 4. 【破坏性】清空中心定义数据
--    授权范围内：中心不迁移旧数据，随后由本地 ExportBundle 推送重建。
--    顺序：子 → 父（外键）。
--    config_meta.version 的归零放在第 5 节末尾 —— config_meta 自身在全新
--    Supabase 项目上还不存在，必须先 CREATE 再 UPDATE。
--
--    ⚠️ 本步无条件清空 6 张定义表。本脚本"可重复运行"指的是不会报错，
--    **不是可以随便重跑**：推送成功后误跑第二遍会静默清空刚灌进去的数据。
--    故加显式闸 —— 真的要清空必须先在同一个会话里 SET。
-- ---------------------------------------------------------------------------
DO $$
BEGIN
    IF current_setting('apiGateway.destructive', true) IS DISTINCT FROM 'yes' THEN
        RAISE EXCEPTION
            '第 4 步会 TRUNCATE 全部 6 张定义表。确认要清空，请先执行：'
            '    SET apiGateway.destructive = ''yes'';'
            ' 若只是想重建表/函数而不清数据，请注释掉第 4 步的 TRUNCATE。';
    END IF;
END $$;

TRUNCATE TABLE lapi_rapi_order, endpoint_credential, lapi, rapi, credential, platform
    RESTART IDENTITY CASCADE;

-- ---------------------------------------------------------------------------
-- 5. 版本号 + commit 级 bump（沿用 v1 机制）
--    语句级触发器：管理端一个逻辑 CRUD 必须放单个事务，
--    否则代理可能拉到中间态（下轮自愈）。见设计 §3。
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS config_meta (
    id         INTEGER PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    version    BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO config_meta (id, version) VALUES (1, 0) ON CONFLICT (id) DO NOTHING;
-- 归零：全新库上面 INSERT 已是 0（幂等），已有中心则重置，
-- 使代理下次轮询看到版本变化而重新全量拉取。必须在 INSERT 之后。
UPDATE config_meta SET version = 0, updated_at = now() WHERE id = 1;

CREATE OR REPLACE FUNCTION bump_version() RETURNS TRIGGER
LANGUAGE plpgsql AS $$
BEGIN
    UPDATE config_meta SET version = version + 1, updated_at = now() WHERE id = 1;
    RETURN NULL;
END;
$$;

CREATE OR REPLACE TRIGGER trg_bump_platform
    AFTER INSERT OR UPDATE OR DELETE ON platform
    FOR EACH STATEMENT EXECUTE FUNCTION bump_version();
CREATE OR REPLACE TRIGGER trg_bump_credential
    AFTER INSERT OR UPDATE OR DELETE ON credential
    FOR EACH STATEMENT EXECUTE FUNCTION bump_version();
CREATE OR REPLACE TRIGGER trg_bump_rapi
    AFTER INSERT OR UPDATE OR DELETE ON rapi
    FOR EACH STATEMENT EXECUTE FUNCTION bump_version();
CREATE OR REPLACE TRIGGER trg_bump_endpoint_credential
    AFTER INSERT OR UPDATE OR DELETE ON endpoint_credential
    FOR EACH STATEMENT EXECUTE FUNCTION bump_version();
CREATE OR REPLACE TRIGGER trg_bump_lapi
    AFTER INSERT OR UPDATE OR DELETE ON lapi
    FOR EACH STATEMENT EXECUTE FUNCTION bump_version();
CREATE OR REPLACE TRIGGER trg_bump_lapi_rapi_order
    AFTER INSERT OR UPDATE OR DELETE ON lapi_rapi_order
    FOR EACH STATEMENT EXECUTE FUNCTION bump_version();

-- ---------------------------------------------------------------------------
-- 6. 拉取接口
--    get_version()：代理轮询，只回版本号（便宜）。
--    get_bundle(p_version)：p_version == 当前版本时返回 NULL（304 等价短路）；
--                           否则返回 {schema_version:2, version, bundle:{...}}。
--
--    [重要] 字段名必须与 internal/bundle/bundle.go 的 json tag 逐字一致：
--      Go 侧 Parse 开了 DisallowUnknownFields，多一个键就会解析失败；
--      少一个键则是零值——同样危险，所以下面每个对象都写全。
--
--    排序：credentials 按 (base_url, sort_order) 以保持平台内轮换序。
--    bindings 只输出**真实存在**的行——"该端点绑定该平台全部凭据"由消费端
--      按"无绑定条目"缺省处理，绝不在中心展开成 N 行（否则凭据增删会让
--      中心数据抖动，详见 bundle_export.go 同处注释）。
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION get_version() RETURNS BIGINT
LANGUAGE sql STABLE AS $$
    SELECT version FROM config_meta WHERE id = 1;
$$;

CREATE OR REPLACE FUNCTION get_bundle(p_version BIGINT DEFAULT NULL) RETURNS JSONB
LANGUAGE sql STABLE AS $$
    WITH cur AS (SELECT version FROM config_meta WHERE id = 1)
    SELECT jsonb_build_object(
        'schema_version', 2,
        'version', cur.version,
        'bundle', jsonb_build_object(
            'platforms', COALESCE((
                SELECT jsonb_agg(jsonb_build_object(
                    'base_url', p.base_url, 'name', p.name, 'token', p.token,
                    'last_token_fetch', p.last_token_fetch, 'enabled', p.enabled,
                    'notes', p.notes, 'supported_formats', p.supported_formats,
                    'format_endpoints', p.format_endpoints, 'custom_headers', p.custom_headers,
                    'billing_address', p.billing_address, 'login_account', p.login_account,
                    'login_password', p.login_password, 'sort_order', p.sort_order
                ) ORDER BY p.sort_order, p.name) FROM platform p
            ), '[]'::jsonb),
            'credentials', COALESCE((
                SELECT jsonb_agg(jsonb_build_object(
                    'token_hash', c.token_hash, 'platform_base_url', p.base_url,
                    'sort_order', c.sort_order, 'token', c.token, 'label', c.label,
                    'enabled', c.enabled, 'expires_at', c.expires_at, 'is_free', c.is_free
                ) ORDER BY p.base_url, c.sort_order)
                FROM credential c JOIN platform p ON p.id = c.platform_id
            ), '[]'::jsonb),
            'rapis', COALESCE((
                SELECT jsonb_agg(jsonb_build_object(
                    'platform_base_url', p.base_url, 'model', r.model, 'alias', r.alias,
                    'enabled', r.enabled, 'base_cost', r.base_cost, 'high_cost', r.high_cost,
                    'rpm_limit', r.rpm_limit, 'rph_limit', r.rph_limit, 'rpd_limit', r.rpd_limit,
                    'tpm_limit', r.tpm_limit, 'tph_limit', r.tph_limit, 'tpd_limit', r.tpd_limit,
                    'time_period_rules', r.time_period_rules, 'supported_formats', r.supported_formats,
                    'custom_headers', r.custom_headers, 'source', r.source,
                    'vendor', r.vendor, 'series', r.series, 'model_name', r.model_name,
                    'version', r.version, 'suffix', r.suffix, 'notes', r.notes,
                    'sort_order', r.sort_order
                ) ORDER BY p.base_url, r.sort_order, r.alias)
                FROM rapi r JOIN platform p ON p.id = r.platform_id
            ), '[]'::jsonb),
            'lapis', COALESCE((
                SELECT jsonb_agg(jsonb_build_object(
                    'alias', l.alias, 'notes', l.notes, 'enabled', l.enabled,
                    'vendor', l.vendor, 'series', l.series, 'model_name', l.model_name,
                    'version', l.version, 'suffix', l.suffix
                ) ORDER BY l.alias) FROM lapi l
            ), '[]'::jsonb),
            'bindings', COALESCE((
                SELECT jsonb_agg(jsonb_build_object(
                    'platform_base_url', p.base_url, 'model', r.model,
                    'token_hash', c.token_hash,
                    'rpm_limit', ec.rpm_limit, 'rph_limit', ec.rph_limit,
                    'rpd_limit', ec.rpd_limit, 'tpm_limit', ec.tpm_limit,
                    'tph_limit', ec.tph_limit, 'tpd_limit', ec.tpd_limit,
                    'enabled', ec.enabled
                ) ORDER BY p.base_url, r.model, c.token_hash)
                FROM endpoint_credential ec
                JOIN rapi r       ON r.id = ec.rapi_id
                JOIN platform p   ON p.id = r.platform_id
                JOIN credential c ON c.id = ec.credential_id
                WHERE c.token_hash IS NOT NULL
            ), '[]'::jsonb),
            'lapi_rapi_order', COALESCE((
                SELECT jsonb_agg(jsonb_build_object(
                    'lapi_alias', l.alias, 'rapi_platform_base_url', p.base_url,
                    'rapi_model', r.model, 'order_index', o.order_index
                ) ORDER BY l.alias, o.order_index)
                FROM lapi_rapi_order o
                JOIN lapi l     ON l.id = o.lapi_id
                JOIN rapi r     ON r.id = o.rapi_id
                JOIN platform p ON p.id = r.platform_id
            ), '[]'::jsonb)
        )
    ) FROM cur
    WHERE p_version IS NULL OR p_version <> cur.version;
$$;

-- ---------------------------------------------------------------------------
-- 7. 权限
--    单写者约定下不启用 RLS。
--    代理（publishable/anon）只需读 + 调 RPC；管理端（service_role/secret）可写。
-- ---------------------------------------------------------------------------
GRANT EXECUTE ON FUNCTION get_version() TO anon, authenticated;
GRANT EXECUTE ON FUNCTION get_bundle(BIGINT) TO anon, authenticated;
GRANT SELECT ON platform, credential, rapi, endpoint_credential, lapi, lapi_rapi_order
    TO anon, authenticated;
