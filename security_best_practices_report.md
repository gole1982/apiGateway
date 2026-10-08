# 安全最佳实践审计报告 — apiGateway

**审计日期**：2026-10-09 · **范围**：全仓库（Go 1.25，net/http 标准库，无 Web 框架）
**基线**：`security-best-practices` 技能 golang-general-backend-security 规范 + 本仓库 [docs/安全模型.md](docs/安全模型.md)

## 摘要

整体安全姿态**良好**：主密钥管理（env 唯一来源、缺 key 拒绝启动）、AES-256-GCM、CSRF originGuard、
面板默认绑回环、日志脱敏、参数化 SQL、中心侧 RLS 写隔离均已落实，且有文档化的威胁模型
（docs/安全模型.md）。未发现 Critical/High 级漏洞。

主要可改进项集中在**资源耗尽面**：无认证的代理端口（0.0.0.0:13579）对请求体无大小上限；
以及若干纵深防御缺口（容器 root 运行、面板缺安全响应头）。

「代理端口无认证」是 README 明文记载的**设计决策**（自部署自用、靠部署位置隔离），本报告不将其
列为缺陷，但相关的 DoS 面（发现 1）仍值得收口。

---

## Medium

### 发现 1 — GO-HTTP-002：代理入口请求体无大小上限（内存耗尽 DoS）

- **位置**：[internal/gateway/gateway.go:648](internal/gateway/gateway.go#L648) `HandleChatCompletions`
- **证据**：`body, err := io.ReadAll(r.Body)` — 无 `http.MaxBytesReader` 或等价上限。
- **影响**：代理端口按设计绑 `0.0.0.0` 且**不做下游认证**（README §口径说明），任何能连到
  13579 的主机可持续发送超大 body；每次 `io.ReadAll` 全量进内存，多并发下直接打爆容器内存
  （docker-compose 未设 mem limit）。LLM 请求体合法大小通常在数 MB 内，一个 32~64MB 的上限
  不会误伤正常流量。
- **修复**（最小改动）：

```go
const maxProxyBodyBytes = 64 << 20 // 按实际上游上下文窗口校准
body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxProxyBodyBytes))
```

  超限返回 413。注意流式路径同样走此入口，一处即可覆盖。
- **缓解**：在 docker-compose.yml 为 gateway 服务加 `mem_limit`；或在反代（nginx）设
  `client_max_body_size`。
- **同类低危**：面板各 `json.NewDecoder(r.Body)` 端点（service.go 约 25 处）也无上限，但面板
  默认绑回环且 compose 只发布到 `127.0.0.1`，风险低；如加 `MaxBytesReader` 中间件可一并收口。

---

## Low

### 发现 2 — 容器以 root 运行

- **位置**：[Dockerfile](Dockerfile)（最终 stage 无 `USER` 指令）
- **影响**：单一 Go 二进制 + Alpine，攻击面小；但一旦进程被攻破即获容器内 root。
  属纵深防御缺口。
- **修复**：runtime stage 加 `RUN adduser -D -u 10001 gateway && USER gateway`；
  注意 `/app/data` volume 挂载点的属主要同步（named volume 首次挂载会继承镜像层属主）。

### 发现 3 — GO-HTTP-001：HTTP Server 未显式设 `ReadHeaderTimeout` / `MaxHeaderBytes`

- **位置**：[internal/service/service.go:291](internal/service/service.go#L291)（proxy）、
  [service.go:322](internal/service/service.go#L322)（web）
- **现状评估**：两者均设了 `ReadTimeout: 30s`，它覆盖「连接接受到 body 读完」全过程，
  慢速 header 攻击实际已被 30s 兜住；`MaxHeaderBytes` 缺省 1MB 也属合理。`WriteTimeout: 0`
  是流式 LLM 响应的刻意设计（注释已写明）。
- **建议**：显式补 `ReadHeaderTimeout: 10s` 作为纵深防御（让慢连接在 header 阶段更早被掐断），
  非必需。优先级低。

### 发现 4 — 日志脱敏的两个边角

- **位置**：[internal/logger/sanitize.go](internal/logger/sanitize.go)
- a) `SanitizeKey` 对 ≤4 字符的值**原样返回**（L7-8），≤8 字符只遮中间两位。短 token/短
  `x-api-key` 会全量落日志。真实 LLM key 都远超 8 字符，风险低。
- b) `SanitizeHeaders` **无任何调用方**（死代码）——实际生效的是
  [gateway.go:666-670](internal/gateway/gateway.go#L666-L670) 对**所有** header 值逐个
  `SanitizeKey`，覆盖面反而更全。
- **修复**：删除 `SanitizeHeaders`；`SanitizeKey` 的短值分支改为全遮蔽（如 `****`）。

### 发现 5 — GO-HTTP-004：面板缺安全响应头（纵深防御）

- **位置**：[internal/service/service.go:685-695](internal/service/service.go#L685-L695)（`/` 返回 dashboardHTML）
- **现状**：仅有 `Cache-Control: no-store`；无 `X-Content-Type-Options: nosniff`、
  无 `X-Frame-Options` / CSP `frame-ancestors`。dashboard.html 全站未用 `innerHTML`/
  `insertAdjacentHTML`（已核实），XSS 面小；面板默认绑回环，故定 Low。
- **修复**：`/` 响应补两行头即可；若将来面板被反代暴露，再评估 CSP `script-src`（当前为单文件
  内联 JS，需要 `unsafe-inline` 或 hash 白名单，改造量大，可缓）。

---

## Info（已知 / 设计内，无需动作）

1. **代理端口与面板 API 全部无认证** — README §端口表与安全说明明文记载的设计决策
   （自部署自用），部署侧职责已写清：面板只发布回环、代理端口勿暴露公网。符合
   docs/安全模型.md §1「准入靠部署位置而非身份层」。
2. **govulncheck job `continue-on-error: true`** — 仓库已自评（security.yml TODO：
   存量清零后改阻断）。建议按计划收口。
3. **CI 未启用 `-race`** — ci.yml 注释已标明为第三阶段计划，属进行中的工作。
4. **上游 HTTP client `Timeout: 0`** — gateway.go:104-108 有充分注释：流式请求不能设全局
   deadline，dial/TLS 已由 Transport 限时，非流式走 per-request context。判定为合理设计。
5. **`math/rand` 用于日志关联 id** — logger.go:461-467 注释 + `//nolint:gosec` 理由成立
   （不参与鉴权），会话 id 走 crypto/rand 的 uuid。符合 GO-CRYPTO-001。
6. **gosec G704/G705 三处 `#nosec`** — 上游 URL 来自管理员配置的 platform endpoint，
   非客户端输入；属架构性误报，内联豁免理由已写明，处理正确。
7. **`fmt.Sprintf` 拼 SQL**（db.go 若干处）— 拼接对象全部是迁移代码内部的表名/列名常量，
   无用户输入进入；数据操作均走 `?` 占位符。判定安全。
8. **加密实现** — AES-256-GCM + crypto/rand nonce、主密钥仅 env 来源、缺 key fail-closed、
   旧文件密钥仅作迁移提示不读取。符合 GO-CONFIG-001 / GO-CRYPTO-001。
9. **CSRF** — originGuard（service.go:628）按 (scheme, host, port) 三元组判同源，写方法才拦，
   面板无 cookie 认证，防护模型与威胁面匹配。
10. **供应链** — go.sum 已提交；CI 未关 GOSUMDB；Dockerfile GOPROXY 用官方默认值，
    可选 goproxy.cn 仅作网络加速且 fallback `direct` 仍走 checksum 校验。无 GO-SUPPLY-001 问题。
11. **无 pprof/expvar/调试端点暴露**；`proxy.cfg`/`proxy.docker.cfg` 仅为模板，无真实密钥
    入库；`.env` 已 gitignore。

---

## 建议的处理顺序

1. 发现 1（代理 body 上限）— 唯一影响可用性的 Medium，改动一行。
2. 发现 4（删死代码 + 短 key 脱敏）— 顺手级。
3. 发现 2（容器非 root）— 需验证 volume 属主，单独一个提交。
4. 发现 3、5 — 可选的纵深防御。

如需我开始修复，请指明从哪一项开始。
