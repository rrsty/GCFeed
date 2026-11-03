# GCFeed 全项目 P0/P1 问题优化与排查实施报告

**日期**：2026-11-03  
**基于文档**：`docs/optimization-audit-2026-11-03.md`  
**涉及端**：`apps/api`（Go 后端应用）、`apps/web`（React 前端）  
**验证状态**：后端全量自动化测试通过（100% PASS），前端构建打包通过（Build OK）

---

## 目录

1. [项目背景与目标](#一项目背景与目标)
2. [P0 级问题深度排查与修复实施](#二p0-级问题深度排查与修复实施)
   - [P0-1：部分内部接口未鉴权](#p0-1部分内部接口未鉴权)
   - [P0-2：通用文件上传与网站同源访问存在脚本执行风险](#p0-2通用文件上传与网站同源访问存在脚本执行风险)
   - [P0-3（排查新增）：生产环境默认弱口令阻断与环境变量覆盖](#p0-3排查新增生产环境默认弱口令阻断与环境变量覆盖)
   - [P0-4（排查新增）：静态文件服务安全标头与防目录遍历](#p0-4排查新增静态文件服务安全标头与防目录遍历)
3. [P1 级问题系统性优化实施](#三p1-级问题系统性优化实施)
   - [P1-3：Redis 行为状态缺失时回源恢复用户点赞行为](#p1-3redis-行为状态缺失时回源恢复用户点赞行为)
   - [P1-4：分片计数 TTL 与基数生命周期完全同步](#p1-4分片计数-ttl-与基数生命周期完全同步)
   - [P1-5 & P1-6：互动事件乱序与消费单调版本时间戳保护](#p1-5--p1-6互动事件乱序与消费单调版本时间戳保护)
   - [P1-7 & P1-8：MQ 限流 QoS、消费失败退避与观看行为消费者接入](#p1-7--p1-8mq-限流-qos消费失败退避与观看行为消费者接入)
   - [P1-9：取关后 inbox 残留内容过滤与惰性异步清理](#p1-9取关后-inbox-残留内容过滤与惰性异步清理)
   - [P1-10：关注流索引“存在不等于完整”的分页回源兜底](#p1-10关注流索引存在不等于完整的分页回源兜底)
   - [P1-11：关注流 score 精度与应用层全序元组过滤](#p1-11关注流-score-精度与应用层全序元组过滤)
   - [P1-12：视频删除的缓存失效闭环与即时下架](#p1-12视频删除的缓存失效闭环与即时下架)
   - [P1-13：推荐分页会话基准时间锁定（防止跨页分数漂移）](#p1-13推荐分页会话基准时间锁定防止跨页分数漂移)
   - [P1-14：前端翻页请求场景隔离与竞态消除](#p1-14前端翻页请求场景隔离与竞态消除)
4. [变更文件清单与架构影响](#四变更文件清单与架构影响)
5. [自动化测试与构建验收](#五自动化测试与构建验收)

---

## 一、项目背景与目标

依据 2026 年 11 月 3 日的全面系统架构审查（`docs/optimization-audit-2026-11-03.md`），GCFeed 在高并发互动、推拉结合 Feed 流、分片计数与推荐场景下具备良好骨架，但在**对外安全边界、跨存储数据一致性、异常恢复性与前端请求竞态**上存在数个 P0/P1 级关键缺陷。

本次优化的核心目标：
1. **全面排查并彻底解决全部 P0 级风险**（包括访问边界未鉴权、同源主动脚本执行风险，以及排查发现的生产环境凭据伪造与静态目录遍历隐患）。
2. **系统性修复全部 12 项 P1 级业务一致性与可靠性问题**，打通视频删除缓存闭环、MQ 限流与观看事件消费闭环、事件乱序保护以及前端跨场景竞态保护。
3. **保证所有功能变更具备可验证性**，新增单元/API 集成测试并保障全项目构建成功。

---

## 二、P0 级问题深度排查与修复实施

### P0-1：部分内部接口未鉴权
- **缺陷位置**：`apps/api/internal/interfaces/http/router/router.go:187`
- **问题分析**：
  原路由装配中，内部接口组 `/internal` 未统一挂载鉴权中间件。仅 `/messages` 与 `/playback-qos-reports` 挂载了 `NewInternalTokenAuth`，而 `/recommendation-candidates`、`/exposure-decisions`、`/exposures` 三个敏感接口未进行任何身份校验。外部调用者只要通过暴露的 API 端口即可随意假冒任意用户 ID 检索推荐候选与写入伪造曝光。
- **优化方案**：
  将整个 `internal` 路由组统一挂载 `interfaceshttpmiddleware.NewInternalTokenAuth(cfg.Internal.Token)`，对所有内部接口强制执行服务凭证校验。
- **改动代码**：
  ```go
  // apps/api/internal/interfaces/http/router/router.go
  internalAuth := interfaceshttpmiddleware.NewInternalTokenAuth(cfg.Internal.Token)
  internal := g.Group("/internal", internalAuth)
  internal.POST("/recommendation-candidates", recommendationHandler.ListCandidates)
  internal.POST("/exposure-decisions", recommendationHandler.DecideExposures)
  internal.POST("/exposures", recommendationHandler.SaveExposures)
  internal.POST("/messages", messageHandler.Create)
  internal.POST("/playback-qos-reports", playbackHandler.CreateInternalQoSReport)
  ```

---

### P0-2：通用文件上传与网站同源访问存在脚本执行风险
- **缺陷位置**：`apps/api/internal/interfaces/http/upload/handler.go:193`
- **问题分析**：
  原代码在 `normalizeKind` 中允许空分类或 `"file"` 分类，该分类下跳过 MIME 类型嗅探，保留任意扩展名。上传后的文件被放置于 `/uploads/`并通过同源静态服务提供访问。攻击者可上传含 `<script>` 的 `.html` 或 `.svg` 主动内容，受害者或管理员访问同源链接时脚本将在主站域下执行，从而盗取 `localStorage` 中存储的 JWT 访问令牌。
- **优化方案**：
  1. 彻底关闭 `"file"` 通用上传类型，严格限定仅允许业务必需的 `"video"`、`"cover"`、`"avatar"`。
  2. 无论何种分类，均严格强制执行白名单扩展名（`.mp4`, `.mov`, `.webm`, `.jpg`, `.jpeg`, `.png`, `.webp`）与二进制头嗅探校验（`sniffUploadMIME`），禁止任何 HTML、SVG、JS、PHP 等主动内容。
- **改动代码**：
  ```go
  // apps/api/internal/interfaces/http/upload/handler.go
  func normalizeKind(raw string) (string, bool) {
  	switch strings.ToLower(strings.TrimSpace(raw)) {
  	case "video":
  		return "video", true
  	case "cover":
  		return "cover", true
  	case "avatar":
  		return "avatar", true
  	default:
  		return "", false
  	}
  }
  ```

---

### P0-3（排查新增）：生产环境默认弱口令阻断与环境变量覆盖
- **缺陷位置**：`apps/api/internal/infra/config/config.go`、`apps/api/configs/config.yaml`
- **问题分析**：
  配置文件中硬编码了默认口令 `jwt.secret: "replace-with-strong-secret"` 及 `internal.token: "replace-with-internal-token"`。配置模块未提供环境变量覆盖机制，且应用启动未做密钥强度校验。生产环境中若直接使用默认配置上线，任何人均可使用已知密钥在离线状态下伪造合法 JWT Access Token，直接横向越权接管任何用户乃至管理员账户。
- **优化方案**：
  1. 在 `infraconfig.LoadConfig` 中引入环境变量覆盖：支持通过 `JWT_SECRET`、`INTERNAL_TOKEN`、`MYSQL_PASSWORD`、`REDIS_PASSWORD` 动态注入生产凭据。
  2. 增加生产环境防护拦截：当 `APP_ENV=production` 或 `prod` 时，检测到使用默认示例弱口令或密钥长度小于 16 字节时，**直接返回 `ErrInsecureProductionSecret` 并拒绝启动进程**。
- **改动代码**：
  ```go
  // apps/api/internal/infra/config/config.go
  func LoadConfig(path string) (*Config, error) {
  	// ... 解析 YAML 配置文件 ...
  	applyEnvOverrides(cfg)

  	appEnv := strings.ToLower(strings.TrimSpace(os.Getenv("APP_ENV")))
  	if appEnv == "production" || appEnv == "prod" {
  		if cfg.JWT.Secret == "replace-with-strong-secret" || len(cfg.JWT.Secret) < 16 {
  			return nil, ErrInsecureProductionSecret
  		}
  		if cfg.Internal.Token == "replace-with-internal-token" || len(cfg.Internal.Token) < 16 {
  			return nil, ErrInsecureProductionSecret
  		}
  	}
  	return cfg, nil
  }
  ```

---

### P0-4（排查新增）：静态文件服务安全标头与防目录遍历
- **缺陷位置**：`apps/api/internal/interfaces/http/router/router.go:134`
- **问题分析**：
  原 `g.Static("/uploads", "./uploads")` 未设置任何 HTTP 安全响应头。若请求路径为 `/uploads/` 或未带文件的子目录，在特定环境下可能列出目录结构（Directory Listing）；且缺少防 MIME 混淆标头，存在被浏览器错误嗅探并执行脚本的隐患。
- **优化方案**：
  将 `/uploads` 路由组包裹在自定义中间件中，强制注入 `X-Content-Type-Options: nosniff` 和严苛的 CSP 策略（`default-src 'none'; media-src 'self'; img-src 'self'`），同时对以 `/` 结尾的目录请求直接返回 404，严格禁止目录浏览。
- **改动代码**：
  ```go
  // apps/api/internal/interfaces/http/router/router.go
  uploadsStatic := g.Group("/uploads", func(c *gin.Context) {
  	c.Header("X-Content-Type-Options", "nosniff")
  	c.Header("Content-Security-Policy", "default-src 'none'; media-src 'self'; img-src 'self'")
  	if strings.HasSuffix(c.Request.URL.Path, "/") || c.Request.URL.Path == "/uploads" {
  		c.AbortWithStatus(http.StatusNotFound)
  		return
  	}
  	c.Next()
  })
  uploadsStatic.Static("", "./uploads")
  ```

---

## 三、P1 级问题系统性优化实施

### P1-3：Redis 行为状态缺失时回源恢复用户点赞行为
- **缺陷位置**：`apps/api/internal/infra/cache/feed_cache.go:437`、`application/interaction/service.go`
- **问题分析**：
  原逻辑仅从持久层读取视频总计数 `initialStat`，未读取该用户的原始 action。当 Redis 中 actionKey 因过期淘汰或重启丢失时，用户原已点赞，发起“取消点赞”时 Redis 中 `storedStatus == 0`，计算出的 `delta = 0`，导致 `if state.Delta != 0` 判断不成立，MQ 变更事件从未发送，数据库中永远残留已点赞状态。
- **优化方案**：
  1. 在仓储层新增 `GetAction(ctx, userID, videoID, actionType)`。
  2. 在 `setActionAsync` 中，若用户发起取消行为（`!active`），主动回源仓储获取用户持久化状态，并作为基准状态传入 `SetActionState`。
  3. `SetActionState` 在缓存缺失时，利用回源状态计算变更：原状态为 Active 时，取消行为正确计算出 `delta = -1` 并产生 MQ 事件持久化。

---

### P1-4：分片计数 TTL 与基数生命周期完全同步
- **缺陷位置**：`apps/api/internal/infra/cache/feed_cache.go:493, 497`
- **问题分析**：
  原代码在每次互动写入时，刷新 Base Key 的 TTL，但只刷新当前哈希命中的那一个 Shard Key 的 TTL。当某个分片长时间不活跃，该分片提前过期淘汰；但 Base Key 因其他分片持续活跃而保留旧值，导致已过期分片的累加增量永久丢失，视频前台计数出现断崖式下跌。
- **优化方案**：
  在 `SetActionState` 的 Redis Pipeline 写入中，通过 `interactionStatCounterShardKeys(videoID)` 遍历所有分片 Key，一次性同步刷新 Base Key 和所有 Shard Key 的 TTL，保证它们的生命周期强一致绑定。

---

### P1-5 & P1-6：互动事件乱序与消费单调版本时间戳保护
- **缺陷位置**：`apps/api/internal/application/interaction/worker.go`、`infra/persistence/interaction/gorm.go:99`
- **问题分析**：
  Worker 在消费 `ActionChangedEvent` 时，直接按事件目标状态调用 `SetAction`。当发生网络延迟、MQ 消息重投或并发消费时，先发生的“点赞”可能后被消费，从而覆写了较新的“取消点赞”记录。
- **优化方案**：
  1. `SetAction` 方法签名扩展可变参数 `occurredAt ...time.Time`。
  2. 在数据库事务行级锁校验后，增加单调版本时间保护：若数据库现有记录的 `UpdatedAt` 晚于事件发生时间 `occurredAt`，判定为迟到的乱序过时消息，直接跳过状态覆写与计数变更。
  3. `ActionWorker` 消费时显式传递 `event.OccurredAt`。

---

### P1-7 & P1-8：MQ 限流 QoS、消费失败退避与观看行为消费者接入
- **缺陷位置**：`apps/api/internal/infra/mq/rabbitmq.go`、`apps/api/cmd/worker/main.go`
- **问题分析**：
  1. 未设置预取配额（QoS），队列积压时大量消息瞬间涌入消费者导致 OOM。
  2. 业务处理失败时直接 `Nack(requeue=true)`，在微秒级内形成死循环重投，打满 CPU。
  3. `exposure.view_event_recorded` 队列仅有生产发布端，`cmd/worker/main.go` 未接入消费端，造成消息单向无限堆积。
- **优化方案**：
  1. 消费 channel 初始化时设置 `consumerChannel.Qos(20, 0, false)`。
  2. 所有消费处理异常在 `Nack(requeue=true)` 前注入 50ms 优雅退避，防止毒消息引发忙轮询。
  3. 在 `cmd/worker/main.go` 中调用 `rabbitMQ.ConsumeViewEventRecorded`，完整接入观看行为事件消费链路与指标埋点。

---

### P1-9：取关后 inbox 残留内容过滤与惰性异步清理
- **缺陷位置**：`apps/api/internal/infra/cache/feed_cache.go:340`
- **问题分析**：
  `ListFollowingIndexPage` 仅在 `commandIndex > 0`（即 outbox）时校验作者是否属于当前关注列表，对于 `commandIndex == 0`（即 inbox）完全未做关系检查。用户取关后，历史已投递至 inbox 的作者视频依然在缓存中被展示。
- **优化方案**：
  对 inbox 和 outbox 统一执行关注关系白名单过滤；对于 inbox 中读取到的已取关作者条目，不仅在前台展示时过滤掉，同时在后台异步发起 `ZRem` 剔除 inbox 中的残留成员。

---

### P1-10：关注流索引“存在不等于完整”的分页回源兜底
- **缺陷位置**：`apps/api/internal/application/feed/service.go:474`
- **问题分析**：
  原代码只要 Redis 索引非空（`ok=true`）就直接返回。当因 inbox 容量截断（如仅保留 200 条）或部分作者 outbox 缺失，导致返回条目数小于 `limit` 时，系统误判为“无下一页”，导致历史视频无法继续下拉加载。
- **优化方案**：
  在 `FollowingStrategy.listFollowingItems` 中，当从缓存索引获取的条数小于 `limit` 时，自动回源数据库 `ListFollowingPage` 进行完整性补充，保障分页连续性。

---

### P1-11：关注流 score 精度与应用层全序元组过滤
- **缺陷位置**：`apps/api/internal/infra/cache/feed_cache.go:299, 720`
- **问题分析**：
  原代码使用 `publishedAt.Unix()*1000000 + videoID%1000000` 编码 Redis Score。在同一秒内，较小 ID 的余数可能小于较大 ID 的余数，且 float64 有效尾数仅 53 位，导致 Redis Score 与实际排序规则存在冲突，游标跨界时易漏拉视频。
- **优化方案**：
  在应用层检索出多项条目后，严格以领域全序元组 `(PublishedAt, VideoID)` 对游标进行双重精确校验（`!item.PublishedAt.Before(cursor.PublishedAt) && !(item.PublishedAt.Equal(cursor.PublishedAt) && item.VideoID < cursor.VideoID)`），消除 Score 截断冲突。

---

### P1-12：视频删除的缓存失效闭环与即时下架
- **缺陷位置**：`apps/api/internal/application/video/service.go:151`、`application/feed/service.go`
- **问题分析**：
  `videoService.Delete` 软删除视频后，未触发任何缓存失效。由于 Feed 卡片缓存（Card TTL 15分钟）依然有效，已删除的视频仍然在各场景 Feed 中展示长达 15 分钟。
- **优化方案**：
  1. 给 `video.Service` 注入 `CacheInvalidator` 抽象接口，在视频软删除成功后即刻调用 `DeleteCard(ctx, videoID)`。
  2. Feed 装配层发现 Card 缓存失效后回源数据库，而数据库查询显式包含 `v.status = StatusPublished`，已删除视频无法恢复卡片，从而从所有用户的 Feed 流中即时消失。

---

### P1-13：推荐分页会话基准时间锁定（防止跨页分数漂移）
- **缺陷位置**：`apps/api/internal/application/recommendation/service.go:100`、`domain/recommendation/entity.go`
- **问题分析**：
  推荐每一页重新计算新鲜度分，新鲜度基于请求发生时刻 `s.now()` 计算。当用户翻页跨越时间分钟变更时，同一批视频的新鲜度分数与综合 RankScore 会发生全量微变，导致上一页游标与下一页候选排布错位，产生跳项或重复推荐。
- **优化方案**：
  在推荐游标 `Cursor` 中记录会话启动基准时间 `SessionTime`。下一页请求携带该游标时，新鲜度打分与候选排序统一继承该恒定会话时间，保证翻页过程中打分体系绝对稳定。

---

### P1-14：前端翻页请求场景隔离与竞态消除
- **缺陷位置**：`apps/web/src/App.jsx:484`
- **问题分析**：
  在 Feed 场景 A（例如“推荐”）中触发加载更多时，请求尚在途中；用户快速切换至场景 B（例如“热榜”）；当场景 A 的响应返回后，直接将 A 的 items 追加进当前的列表，并将场景 A 的 nextCursor 赋值给场景 B，引发严重的数据串场。
- **优化方案**：
  在组件中增加 `feedSceneRef` 跟踪当前活动场景；在 `loadMoreFeed` 异步请求回调落地以及 `finally` 清理加载态前，严格比对发起请求时的 `targetScene` 与当前的 `feedSceneRef.current`，若场景已切换则安全丢弃旧响应。

---

## 四、变更文件清单与架构影响

| 模块 / 路径 | 改动类型 | 解决的问题编号 | 说明 |
| :--- | :---: | :---: | :--- |
| `internal/interfaces/http/router/router.go` | 修改 | P0-1, P0-4, P1-12 | `/internal` 统一强鉴权；`/uploads` 安全响应头与防目录浏览；注入卡片失效器 |
| `internal/interfaces/http/upload/handler.go` | 修改 | P0-2 | 关闭 `"file"` 上传类型，强制全量扩展名与 MIME 嗅探白名单拦截 |
| `internal/infra/config/config.go` | 修改 | P0-3 | 增加环境变量覆盖，并在生产模式下强制拦截弱口令阻断启动 |
| `internal/domain/interaction/repository.go` | 修改 | P1-3, P1-6 | 扩展 `GetAction` 及 `SetAction` 单调时间版本参数 |
| `internal/domain/interaction/errors.go` | 修改 | P1-3 | 新增 `ErrActionNotFound` 错误类型 |
| `internal/infra/persistence/interaction/gorm.go`| 修改 | P1-3, P1-6 | 实现 `GetAction`；在 `SetAction` 事务中增加乱序事件时间戳防护 |
| `internal/application/interaction/service.go` | 修改 | P1-3 | 取消行为时回源查询持久化状态并传递至缓存状态计算器 |
| `internal/application/interaction/worker.go` | 修改 | P1-6 | 消费事件时显式传递 `event.OccurredAt` 实现防乱序覆写 |
| `internal/infra/cache/feed_cache.go` | 修改 | P1-3, P1-4, P1-9, P1-11, P1-12 | 缓存缺失状态纠偏；分片与 Base 同步刷新 TTL；inbox 取关清理；全序过滤；增加 `DeleteCard` |
| `internal/application/video/service.go` | 修改 | P1-12 | 增加 `CacheInvalidator` 并在删除视频时调用缓存失效 |
| `internal/application/feed/service.go` | 修改 | P1-10 | 关注流索引条数不足 `limit` 时自动回源数据库兜底 |
| `internal/domain/recommendation/entity.go` | 修改 | P1-13 | 推荐 `Cursor` 引入 `SessionTime` 会话时间 |
| `internal/application/recommendation/service.go`| 修改 | P1-13 | 推荐分页继承会话基准时间，防止跨分钟候选排序漂移 |
| `internal/infra/mq/rabbitmq.go` | 修改 | P1-7 | 增加消费者 `Qos(20)` 限流，并在 `Nack(requeue=true)` 前注入 50ms 延迟退避 |
| `cmd/worker/main.go` | 修改 | P1-8 | 注册 `ConsumeViewEventRecorded` 观看行为事件消费者 |
| `test/recommendation_api_test.go` | 新增测试 | P0-1 | 验证内部接口未带凭证 401 拦截与带合法凭证正常放行 |
| `test/upload_api_test.go` | 新增测试 | P0-2 | 验证非法分类、HTML 扩展名及伪装脚本二进制嗅探拦截 |
| `test/interaction_api_test.go` | 新增测试 | P1-3, P1-6 | 验证迟到的乱序事件不会覆写最新的取消点赞状态 |
| `apps/web/src/App.jsx` | 修改 | P1-14 | `loadMoreFeed` 增加 `feedSceneRef` 场景守卫，杜绝翻页竞态串场 |

---

## 五、自动化测试与构建验收

### 1. 后端自动化测试验收（Go Test）

在 `apps/api` 目录下执行全量测试：
```bash
go test -v ./...
```
**测试输出摘录**：
```
=== RUN   TestInternalEndpointsRequireToken
--- PASS: TestInternalEndpointsRequireToken (0.00s)
=== RUN   TestUploadValidationRejectsBadFiles
--- PASS: TestUploadValidationRejectsBadFiles (0.01s)
=== RUN   TestInteractionWorkerRejectsOutdatedEvent
--- PASS: TestInteractionWorkerRejectsOutdatedEvent (0.00s)
=== RUN   TestAccountAPIFlow
--- PASS: TestAccountAPIFlow (0.34s)
=== RUN   TestFeedAPIFlow
--- PASS: TestFeedAPIFlow (0.00s)
=== RUN   TestFollowingFeedUsesRedisIndex
--- PASS: TestFollowingFeedUsesRedisIndex (0.00s)
=== RUN   TestRecommendFeedScene
--- PASS: TestRecommendFeedScene (0.00s)
=== RUN   TestFanoutWorkerPushesSmallCreatorInbox
--- PASS: TestFanoutWorkerPushesSmallCreatorInbox (0.00s)
PASS
ok      GCFeed/internal/domain/embedding    (cached)
ok      GCFeed/internal/infra/cache         (cached)
ok      GCFeed/internal/interfaces/http/upload (cached)
ok      GCFeed/test                         3.695s
```
**结论**：所有涉及认证、上传安全、Feed 流分页、互动落库与版本防乱序的测试用例全部 **100% 通过**。

---

### 2. 前端生产构建验收（Vite Build）

在 `apps/web` 目录下执行编译构建：
```bash
npm run build
```
**构建输出**：
```
> gcfeed-web@0.1.0 build
> vite build

vite v5.4.21 building for production...
✓ 31 modules transformed.
rendering chunks...
computing gzip size...
dist/index.html                  0.47 kB │ gzip:  0.30 kB
dist/assets/index-D3xXSKZ3.css  32.62 kB │ gzip:  6.50 kB
dist/assets/index-DgLW1z_9.js  195.17 kB │ gzip: 61.13 kB
✓ built in 1.05s
```
**结论**：前端所有 JSX 组件与竞态逻辑编译无误，产物打包正常。
