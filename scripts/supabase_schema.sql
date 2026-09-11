-- ===========================================================================
-- apiGateway 中心库 schema（Supabase Postgres）
-- 对应设计：docs/superpowers/specs/2026-09-03-center-edge-config-sync-design.md
-- 用法：在 Supabase 项目的 SQL Editor 整体执行一次。幂等（可重复跑）。
--
-- 三组件：中心(本文件) / 管理(直写本库) / 代理(轮询 version + 拉 get_bundle)
-- 权威：定义类=本库权威；健康态/遥测=代理本地（不在本库）。
-- ===========================================================================

-- token / login_password 列由管理端用 center_key(AES) 加密后写入；
-- 本库只持密文，不存明文。

-- ---------------------------------------------------------------------------
-- 1. 定义表（镜像现有 SQLite 定义表列子集；去掉 runtime 列：
--    available / unavailable_reason / failure_type / failure_reason / failed_at
--    / key_model_blocks —— 这些是代理本地健康态，不进中心）
--    JSON 类列用 TEXT（JSON-as-TEXT），与现有 SQLite 一致，保证 bundle 结构体
--    一份双向 marshal/unmarshal、类型不漂移。
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS platform (
    id                BIGSERIAL PRIMARY KEY,
    name              TEXT NOT NULL UNIQUE,            -- 业务键
    base_url          TEXT NOT NULL DEFAULT '',
    token             TEXT NOT NULL DEFAULT '',         -- center_key 密文
    last_token_fetch  TIMESTAMPTZ,
    enabled           BOOLEAN NOT NULL DEFAULT TRUE,   -- 中心权威
    notes             TEXT NOT NULL DEFAULT '',
    supported_formats TEXT NOT NULL DEFAULT '["openai"]', -- JSON array as TEXT
    format_endpoints  TEXT NOT NULL DEFAULT '',          -- JSON {format:url} as TEXT
    custom_headers    TEXT NOT NULL DEFAULT '',          -- JSON array as TEXT
    billing_address   TEXT NOT NULL DEFAULT '',
    login_account     TEXT NOT NULL DEFAULT '',
    login_password    TEXT NOT NULL DEFAULT '',          -- center_key 密文
    sort_order        INTEGER NOT NULL DEFAULT 0,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS platform_keys (
    id          BIGSERIAL PRIMARY KEY,
    platform_id BIGINT NOT NULL REFERENCES platform(id) ON DELETE CASCADE,
    key_index   INTEGER NOT NULL DEFAULT 0,              -- 平台内业务键
    token       TEXT NOT NULL DEFAULT '',                -- center_key 密文
    label       TEXT NOT NULL DEFAULT '',
    enabled     BOOLEAN NOT NULL DEFAULT TRUE,           -- 中心权威
    expires_at  TIMESTAMPTZ,
    is_free     BOOLEAN NOT NULL DEFAULT FALSE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(platform_id, key_index)
);

CREATE TABLE IF NOT EXISTS rapi (
    id                BIGSERIAL PRIMARY KEY,
    platform_id       BIGINT NOT NULL REFERENCES platform(id) ON DELETE CASCADE,
    alias             TEXT NOT NULL,                     -- 平台内业务键
    model             TEXT NOT NULL DEFAULT '',
    enabled           BOOLEAN NOT NULL DEFAULT TRUE,    -- 中心权威
    base_cost         INTEGER NOT NULL DEFAULT 0,
    high_cost         INTEGER NOT NULL DEFAULT 0,
    rpm_limit         INTEGER NOT NULL DEFAULT 0,
    rph_limit         INTEGER NOT NULL DEFAULT 0,
    rpd_limit         INTEGER NOT NULL DEFAULT 0,
    tpm_limit         INTEGER NOT NULL DEFAULT 0,
    tph_limit         INTEGER NOT NULL DEFAULT 0,
    tpd_limit         INTEGER NOT NULL DEFAULT 0,
    time_period_rules TEXT NOT NULL DEFAULT '',          -- JSON array as TEXT
    supported_formats TEXT NOT NULL DEFAULT '["openai"]', -- JSON array as TEXT（继承平台）
    custom_headers    TEXT NOT NULL DEFAULT '',          -- JSON array as TEXT
    -- key_ids 现状是 platform_keys.id 的 CSV（本地自增 id，跨实例不可移植）。
    -- TODO（Phase 2/4）：bundle 传输改用 (platform_name, key_index) 业务键引用；
    -- get_bundle 解析、Apply 还原本地 id。当前列保留 TEXT，存中心侧 id CSV。
    key_ids           TEXT NOT NULL DEFAULT '',
    source            TEXT NOT NULL DEFAULT 'manual',  -- auto_discover | manual
    vendor            TEXT NOT NULL DEFAULT '',
    series            TEXT NOT NULL DEFAULT '',
    model_name        TEXT NOT NULL DEFAULT '',
    version           TEXT NOT NULL DEFAULT '',
    suffix            TEXT NOT NULL DEFAULT '',
    notes             TEXT NOT NULL DEFAULT '',
    sort_order        INTEGER NOT NULL DEFAULT 0,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(platform_id, alias)
);

CREATE TABLE IF NOT EXISTS lapi (
    id         BIGSERIAL PRIMARY KEY,
    alias      TEXT NOT NULL UNIQUE,                     -- 业务键
    notes      TEXT NOT NULL DEFAULT '',
    enabled    BOOLEAN NOT NULL DEFAULT TRUE,            -- 中心权威
    vendor     TEXT NOT NULL DEFAULT '',
    series     TEXT NOT NULL DEFAULT '',
    model_name TEXT NOT NULL DEFAULT '',
    version    TEXT NOT NULL DEFAULT '',
    suffix     TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS lapi_rapi_order (
    id          BIGSERIAL PRIMARY KEY,
    lapi_id     BIGINT NOT NULL REFERENCES lapi(id) ON DELETE CASCADE,
    rapi_id     BIGINT NOT NULL REFERENCES rapi(id) ON DELETE CASCADE,
    order_index INTEGER NOT NULL,
    UNIQUE(lapi_id, rapi_id),
    UNIQUE(lapi_id, order_index)
);

CREATE INDEX IF NOT EXISTS idx_rapi_platform ON rapi(platform_id);
CREATE INDEX IF NOT EXISTS idx_lapi_rapi_lapi ON lapi_rapi_order(lapi_id);

-- ---------------------------------------------------------------------------
-- 2. 版本号 + commit 级 bump
--    每个定义表的 INSERT/UPDATE/DELETE 在语句级触发器里把 version +1。
--    关键约束：管理端必须把“一个逻辑 CRUD”（如：建平台 + 加 2 个 key）
--    放在单个 DB 事务里。Postgres MVCC 保证：未提交事务内的多次 bump
--    对代理不可见；代理只在 commit 后看到 version 跳变 + 一个一致的完整快照，
--    不会拉到半应用态。若管理端把一个逻辑 CRUD 拆成多个独立事务（auto-commit），
--    则代理可能拉到中间态（下轮自愈）——故管理端务必用单事务。
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS config_meta (
    id         INTEGER PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    version    BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO config_meta (id, version) VALUES (1, 0)
    ON CONFLICT (id) DO NOTHING;

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
CREATE OR REPLACE TRIGGER trg_bump_platform_keys
    AFTER INSERT OR UPDATE OR DELETE ON platform_keys
    FOR EACH STATEMENT EXECUTE FUNCTION bump_version();
CREATE OR REPLACE TRIGGER trg_bump_rapi
    AFTER INSERT OR UPDATE OR DELETE ON rapi
    FOR EACH STATEMENT EXECUTE FUNCTION bump_version();
CREATE OR REPLACE TRIGGER trg_bump_lapi
    AFTER INSERT OR UPDATE OR DELETE ON lapi
    FOR EACH STATEMENT EXECUTE FUNCTION bump_version();
CREATE OR REPLACE TRIGGER trg_bump_lapi_rapi_order
    AFTER INSERT OR UPDATE OR DELETE ON lapi_rapi_order
    FOR EACH STATEMENT EXECUTE FUNCTION bump_version();

-- ---------------------------------------------------------------------------
-- 3. 拉取接口
--    get_version()：代理定时轮询，只返回版本号（便宜）。
--    get_bundle(p_version)：version 相同返回 NULL（304 等价短路）；不同返回
--    {schema_version, version, bundle:{...}}。bundle 内引用一律业务键
--    （platform.name / (platform_name, key_index) / (platform_name, alias) /
--    lapi.alias / (lapi_alias, rapi_platform_name, rapi_alias)），不用自增 id，
--    便于代理按业务键 upsert 到本地 SQLite（本地 id 与中心不同）。
--    注：rapi.key_ids 当前直接回传中心 id CSV，业务键解析见 Phase 2/4 TODO。
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION get_version() RETURNS BIGINT
LANGUAGE sql STABLE AS $$
    SELECT version FROM config_meta WHERE id = 1;
$$;

CREATE OR REPLACE FUNCTION get_bundle(p_version BIGINT DEFAULT NULL) RETURNS JSONB
LANGUAGE sql STABLE AS $$
    WITH cur AS (SELECT version FROM config_meta WHERE id = 1)
    SELECT jsonb_build_object(
        'schema_version', 1,
        'version', cur.version,
        'bundle', jsonb_build_object(
            'platforms', COALESCE((
                SELECT jsonb_agg(jsonb_build_object(
                    'name', p.name, 'base_url', p.base_url, 'token', p.token,
                    'last_token_fetch', p.last_token_fetch, 'enabled', p.enabled,
                    'notes', p.notes, 'supported_formats', p.supported_formats,
                    'format_endpoints', p.format_endpoints, 'custom_headers', p.custom_headers,
                    'billing_address', p.billing_address, 'login_account', p.login_account,
                    'login_password', p.login_password, 'sort_order', p.sort_order
                ) ORDER BY p.sort_order, p.name) FROM platform p
            ), '[]'::jsonb),
            'platform_keys', COALESCE((
                SELECT jsonb_agg(jsonb_build_object(
                    'platform_name', p.name, 'key_index', k.key_index,
                    'token', k.token, 'label', k.label, 'enabled', k.enabled,
                    'expires_at', k.expires_at, 'is_free', k.is_free
                ) ORDER BY p.name, k.key_index)
                FROM platform_keys k JOIN platform p ON p.id = k.platform_id
            ), '[]'::jsonb),
            'rapis', COALESCE((
                SELECT jsonb_agg(jsonb_build_object(
                    'platform_name', p.name, 'alias', r.alias, 'model', r.model,
                    'enabled', r.enabled, 'base_cost', r.base_cost, 'high_cost', r.high_cost,
                    'rpm_limit', r.rpm_limit, 'rph_limit', r.rph_limit, 'rpd_limit', r.rpd_limit,
                    'tpm_limit', r.tpm_limit, 'tph_limit', r.tph_limit, 'tpd_limit', r.tpd_limit,
                    'time_period_rules', r.time_period_rules, 'supported_formats', r.supported_formats,
                    'custom_headers', r.custom_headers, 'key_ids', r.key_ids, 'source', r.source,
                    'vendor', r.vendor, 'series', r.series, 'model_name', r.model_name,
                    'version', r.version, 'suffix', r.suffix, 'notes', r.notes, 'sort_order', r.sort_order
                ) ORDER BY p.name, r.sort_order, r.alias)
                FROM rapi r JOIN platform p ON p.id = r.platform_id
            ), '[]'::jsonb),
            'lapis', COALESCE((
                SELECT jsonb_agg(jsonb_build_object(
                    'alias', l.alias, 'notes', l.notes, 'enabled', l.enabled,
                    'vendor', l.vendor, 'series', l.series, 'model_name', l.model_name,
                    'version', l.version, 'suffix', l.suffix
                ) ORDER BY l.alias) FROM lapi l
            ), '[]'::jsonb),
            'lapi_rapi_order', COALESCE((
                SELECT jsonb_agg(jsonb_build_object(
                    'lapi_alias', l.alias, 'rapi_platform_name', p.name, 'rapi_alias', r.alias,
                    'order_index', o.order_index
                ) ORDER BY l.alias, o.order_index)
                FROM lapi_rapi_order o
                JOIN lapi l ON l.id = o.lapi_id
                JOIN rapi r ON r.id = o.rapi_id
                JOIN platform p ON p.id = r.platform_id
            ), '[]'::jsonb)
        )
    ) FROM cur
    WHERE p_version IS NULL OR p_version <> cur.version;
$$;

-- 权限：单写者约定下不启用 RLS。默认对 anon / authenticated 开放 RPC 执行。
-- 若要硬隔离（多管理并发，见设计 §9），在此处收紧：
--   REVOKE ALL ON platform, platform_keys, rapi, lapi, lapi_rapi_order, config_meta FROM anon;
--   GRANT EXECUTE ON FUNCTION get_version(), get_bundle(BIGINT) TO anon;
GRANT EXECUTE ON FUNCTION get_version(), get_bundle(BIGINT) TO anon, authenticated;
