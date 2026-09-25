-- ===========================================================================
-- apiGateway 中心库 —— v2 重建后只读校验脚本
-- 用法：重建 + 推送本地数据之后，在 Supabase SQL Editor 执行。
-- 本脚本**只含 SELECT**，不修改任何数据。
--
-- 重建脚本的第 4 步有破坏性闸：首次执行 supabase_schema_v2.sql 前需先
--   SET apiGateway.destructive = 'yes';
-- 否则会 RAISE EXCEPTION 拒绝 TRUNCATE。详见该脚本第 4 节。
--
-- 期望值（来自当前本地 SQLite，生产初始化以本地为准）：
--   platform 19 / credential 34 / rapi 59 / lapi 22
--   endpoint_credential 82 / lapi_rapi_order 33
-- 任何"应为 0"的检查项非 0，都说明重建或推送有问题，不要继续放量。
-- ===========================================================================

-- 1. 各表行数（与本地对拍）
SELECT 'platform'           AS table_name, count(*) AS rows FROM platform
UNION ALL SELECT 'credential',          count(*) FROM credential
UNION ALL SELECT 'rapi',                count(*) FROM rapi
UNION ALL SELECT 'lapi',                count(*) FROM lapi
UNION ALL SELECT 'endpoint_credential', count(*) FROM endpoint_credential
UNION ALL SELECT 'lapi_rapi_order',     count(*) FROM lapi_rapi_order
ORDER BY 1;

-- 2. 中心版本号（代理靠它判断是否需要重新拉取）
SELECT version AS center_version, updated_at FROM config_meta WHERE id = 1;

-- 3. 业务键唯一性（必须全为 0）—— 重建后应由唯一索引兜住；
--    显式再查一遍是为了发现"索引根本没建上"。
SELECT 'dup_platform_base_url' AS check_name, count(*) AS violations FROM (
    SELECT base_url FROM platform WHERE base_url <> '' GROUP BY base_url HAVING count(*) > 1) x
UNION ALL
SELECT 'dup_credential_token_hash', count(*) FROM (
    SELECT token_hash FROM credential WHERE token_hash IS NOT NULL GROUP BY token_hash HAVING count(*) > 1) x
UNION ALL
SELECT 'dup_rapi_platform_model', count(*) FROM (
    SELECT platform_id, model FROM rapi WHERE model <> '' GROUP BY platform_id, model HAVING count(*) > 1) x
UNION ALL
SELECT 'dup_lapi_alias', count(*) FROM (
    SELECT alias FROM lapi GROUP BY alias HAVING count(*) > 1) x
UNION ALL
SELECT 'dup_lapi_order_index', count(*) FROM (
    SELECT lapi_id, order_index FROM lapi_rapi_order GROUP BY lapi_id, order_index HAVING count(*) > 1) x
ORDER BY 1;

-- 4. 引用完整性（必须全为 0）
SELECT 'rapi_without_platform' AS check_name, count(*) AS violations
FROM rapi r WHERE NOT EXISTS (SELECT 1 FROM platform p WHERE p.id = r.platform_id)
UNION ALL
SELECT 'credential_without_platform', count(*)
FROM credential c WHERE NOT EXISTS (SELECT 1 FROM platform p WHERE p.id = c.platform_id)
UNION ALL
SELECT 'binding_without_rapi', count(*)
FROM endpoint_credential ec WHERE NOT EXISTS (SELECT 1 FROM rapi r WHERE r.id = ec.rapi_id)
UNION ALL
SELECT 'binding_without_credential', count(*)
FROM endpoint_credential ec WHERE NOT EXISTS (SELECT 1 FROM credential c WHERE c.id = ec.credential_id)
UNION ALL
SELECT 'binding_without_token_hash', count(*)
FROM endpoint_credential ec JOIN credential c ON c.id = ec.credential_id WHERE c.token_hash IS NULL
UNION ALL
-- 跨平台绑定：credential.platform_id 只是"归属平台"、不是身份，schema 层面
-- 并不禁止把平台 B 的凭据绑到平台 A 的端点上。但 v1 结构性不可能出现这种
-- 绑定（key_index 是平台内序号），v2 可以。一旦出现，gateway 的
-- filterKeysForRAPIs 会把多个平台的凭据混进同一个池，而 PickAvailableKey
-- 的轮询游标只取 sorted[0].PlatformID —— 配额会记到错误的平台上。
-- 本地迁移产出的绑定天然同平台，所以这里期望恒为 0。
SELECT 'binding_cross_platform', count(*)
FROM endpoint_credential ec
JOIN rapi r       ON r.id = ec.rapi_id
JOIN credential c ON c.id = ec.credential_id
WHERE c.platform_id <> r.platform_id
UNION ALL
SELECT 'order_without_lapi', count(*)
FROM lapi_rapi_order o WHERE NOT EXISTS (SELECT 1 FROM lapi l WHERE l.id = o.lapi_id)
UNION ALL
SELECT 'order_without_rapi', count(*)
FROM lapi_rapi_order o WHERE NOT EXISTS (SELECT 1 FROM rapi r WHERE r.id = o.rapi_id)
ORDER BY 1;

-- 5. 唯一索引是否真的建上了（应返回 3 行）
SELECT indexname, indexdef FROM pg_indexes
WHERE tablename IN ('platform', 'credential', 'rapi')
  AND indexname IN ('idx_platform_base_url', 'idx_rapi_platform_model', 'idx_credential_token_hash')
ORDER BY indexname;

-- 6. 契约冒烟：get_bundle 能否按 v2 形状产出，字段名是否与 Go 契约一致
--    has_null_bundle=false 说明 p_version=NULL 时正常返回全量。
--    key_ids/platform_keys 的出现次数必须为 0（契约外字段，Go 侧会拒收）。
SELECT
    (get_bundle(NULL)->>'schema_version')  AS schema_version,
    (get_bundle(NULL)->'bundle' ? 'credentials')     AS has_credentials,
    (get_bundle(NULL)->'bundle' ? 'bindings')        AS has_bindings,
    (get_bundle(NULL)->'bundle' ? 'lapi_rapi_order') AS has_lapi_rapi_order,
    length(get_bundle(NULL)::text)                   AS payload_bytes,
    (length(get_bundle(NULL)::text)
       - length(replace(get_bundle(NULL)::text, 'key_ids', ''))) / length('key_ids')          AS key_ids_occurrences,
    (length(get_bundle(NULL)::text)
       - length(replace(get_bundle(NULL)::text, 'platform_keys', ''))) / length('platform_keys') AS platform_keys_occurrences;

-- 7. 304 等价短路：传当前版本号应返回 NULL（has_null_bundle=true）
SELECT (get_bundle((SELECT version FROM config_meta WHERE id = 1)) IS NULL) AS has_null_bundle;

-- 8. 抽样对拍：确认自然键拼装正确（platform_base_url 而非 platform_id）
SELECT c.token_hash, p.base_url AS platform_base_url, c.sort_order, c.label, c.enabled
FROM credential c JOIN platform p ON p.id = c.platform_id
ORDER BY p.base_url, c.sort_order
LIMIT 10;

SELECT p.base_url AS platform_base_url, r.model, c.token_hash, ec.enabled, ec.rpm_limit
FROM endpoint_credential ec
JOIN rapi r       ON r.id = ec.rapi_id
JOIN platform p   ON p.id = r.platform_id
JOIN credential c ON c.id = ec.credential_id
ORDER BY p.base_url, r.model, c.token_hash
LIMIT 10;

-- 9. 端点抽样：确认 (base_url, model) 拼装
SELECT p.base_url AS platform_base_url, r.model, r.alias
FROM rapi r JOIN platform p ON p.id = r.platform_id
ORDER BY p.base_url, r.model
LIMIT 10;
