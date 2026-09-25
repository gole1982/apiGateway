-- ===========================================================================
-- 手动录入示例：第一批配置怎么塞进中心 Supabase（管理端上线前的里程碑路径）
-- 配合 scripts/supabase_schema_v2.sql 使用：先跑 v2 schema，再按此模板录入。
-- 注意：本模板按 v1 表结构（platform_keys / key_index）编写，v2 需改为
-- credential（token_hash / sort_order）。仅作字段取值参考，勿直接用于 v2 中心。
-- 录入后 config_meta.version 由触发器自动 +1，各代理 60s 内自动拉到。
--
-- 注意：
--  * 一个平台 + 它的 key + 模型要放在同一个事务里执行（Supabase SQL Editor
--    里整段选中一起跑就是单事务）——代理只在 commit 后看到 version 跳变，
--    不会拉到半应用态。
--  * center_key 留空（明文模式）时 token 直接写明文；若配了 center_key，
--    这里要先加密再写入（管理端 Phase 4 会自动做）。
--  * 引用一律用中心库 id（SELECT 里查），代理拉取后自行解析为本地 id。
-- ===========================================================================

-- ---------- 平台 ----------
INSERT INTO platform (name, base_url, token, supported_formats)
VALUES ('zai', 'https://api.z.ai', '你的平台token', '["openai","anthropic"]');

-- ---------- 该平台的密钥（platform_id 引用上面插入的行）----------
INSERT INTO platform_keys (platform_id, key_index, token, label, is_free)
SELECT id, 0, '你的key明文或密文', '主力key', false FROM platform WHERE name = 'zai';

-- ---------- 上游模型（rapi）----------
INSERT INTO rapi (platform_id, alias, model, key_ids, source)
SELECT id, 'glm-4.7', 'glm-4.7', '0', 'manual' FROM platform WHERE name = 'zai';

-- ---------- 客户端模型（lapi）----------
INSERT INTO lapi (alias, vendor, series, version) VALUES ('glm-4.7', 'zai', 'glm', '4.7');

-- ---------- 路由链：lapi → rapi（failover 顺序）----------
INSERT INTO lapi_rapi_order (lapi_id, rapi_id, order_index)
SELECT l.id, r.id, 0
FROM lapi l, rapi r, platform p
WHERE l.alias = 'glm-4.7' AND r.alias = 'glm-4.7' AND p.name = 'zai' AND r.platform_id = p.id;

-- ---------- 验证 ----------
-- SELECT get_version();  -- 应大于 0（触发器已 bump）
-- SELECT get_bundle(NULL);  -- 应返回完整 JSON（可肉眼检查）
