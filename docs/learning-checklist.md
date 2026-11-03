# GCFeed 深度学习通关清单 (Learning Checklist)

> 这是一个面向 **GCFeed (短视频高并发 Feed 流系统)** 的结构化学习追踪清单。  
> 每一项都设计为可勾选的 `[ ]` 任务，涵盖**核心代码文件定位、设计原理解析、验证命令与自测思考题**。  
> 建议按阶段顺序推进，完成一项勾选一项（将 `[ ]` 改为 `[x]`）。

---

## 阶段 0：环境启动与核心业务体感 (预计耗时: 0.5 天)

**目标**：把系统完整跑起来，通过自动化测试与浏览器交互建立端到端的直观认知。

- [ ] **0.1 浏览官方精简指南**
  - [ ] 阅读 [docs/quickread.md](file:///c:/Users/rsty/Desktop/gofeed/GCFeed/docs/quickread.md)，建立“用户/内容/分发/互动”四条主线认知
  - [ ] 阅读 [docs/architecture.md](file:///c:/Users/rsty/Desktop/gofeed/GCFeed/docs/architecture.md)，掌握系统上下文与分层图
- [ ] **0.2 验证本地 Go 测试套件**
  - [ ] 在项目根目录执行完整测试：
    ```bash
    go test -v -C "apps/api" ./...
    ```
  - [ ] 观察测试输出，重点查看 `test/*_api_test.go` 如何在无需外部 MySQL/Redis 依赖下使用内存仓储完成全流程测试
- [ ] **0.3 使用 Docker Compose 启动全套服务**
  - [ ] 确保本地 Docker Desktop 已启动
  - [ ] 启动后端 API、Worker、前端 Web、MySQL、Redis、RabbitMQ、Prometheus、Grafana：
    ```bash
    cd apps
    docker compose up -d --build
    ```
  - [ ] 验证服务健康检查：
    - API 健康探针: `http://127.0.0.1:8080/health`
    - API 指标采集: `http://127.0.0.1:8080/metrics`
    - RabbitMQ 后台: `http://127.0.0.1:15672` (默认 guest / guest)
    - Grafana 面板: `http://127.0.0.1:3000` (默认 admin / admin)
- [ ] **0.4 浏览器端体验完整业务闭环**
  - [ ] 打开前端页面 `http://127.0.0.1:5173`
  - [ ] 注册新用户并完成登录
  - [ ] 上传短视频并发布
  - [ ] 切换各个 Feed 流 Tab（最新 Timeline、热门 Hot、推荐 Recommend、关注 Following）
  - [ ] 完成一次点赞、收藏、发表评论，并在个人中心查看
- [ ] **阶段 0 自测思考题**：
  > 1. 发布视频时，文件上传接口和发布接口是同一个吗？为什么短视频通常采用上传与发布分离？  
  > 2. 刷新页面时，网络面板中请求的 `/api/feed-items` 携带了哪些查询参数？

---

## 阶段 1：项目骨架与依赖装配机制 (预计耗时: 0.5 天)

**目标**：理解后端 Clean Architecture 四层分层，搞懂服务启动时依赖如何一步步注册并组装。

- [ ] **1.1 理解四层架构规范**
  - [ ] 阅读 [docs/engineering.md](file:///c:/Users/rsty/Desktop/gofeed/GCFeed/docs/engineering.md) 了解代码规范与目录划分
  - [ ] 弄清四层职责与依赖方向：
    - `Domain`（纯领域实体、仓储接口，不依赖第三方库）
    - `Application`（用例编排、分页游标、跨模块调用）
    - `Infrastructure`（GORM 持久化、Redis 缓存、RabbitMQ 客户端）
    - `Interfaces`（Gin 路由、中间件、DTO、状态码映射）
- [ ] **1.2 精读 API 服务启动入口**
  - [ ] 打开 [apps/api/cmd/feed/main.go](file:///c:/Users/rsty/Desktop/gofeed/GCFeed/apps/api/cmd/feed/main.go)
  - [ ] 理顺主流程：`LoadConfig` $\to$ `infradatabase.New` $\to$ `infrahttpgin.Init` $\to$ `router.Register` $\to$ `infrahttpgin.Run`
- [ ] **1.3 核心攻坚：装配中心 `router.Register`**
  - [ ] 逐行阅读 [apps/api/internal/interfaces/http/router/router.go](file:///c:/Users/rsty/Desktop/gofeed/GCFeed/apps/api/internal/interfaces/http/router/router.go)
  - [ ] 绘制或整理出依赖注入链条：
    ```text
    Config -> DB/Redis/RabbitMQ/JWT -> Repository -> Service (Functional Options) -> Handler -> Gin Router
    ```
  - [ ] 理解为什么使用 Functional Options（如 `applicationfeed.WithFeedCache(...)`）来动态装配中间件与缓存
- [ ] **1.4 精读 Worker 进程启动入口**
  - [ ] 打开 [apps/api/cmd/worker/main.go](file:///c:/Users/rsty/Desktop/gofeed/GCFeed/apps/api/cmd/worker/main.go)
  - [ ] 理解 Worker 独立进程模型：单独拉起 `ActionWorker`、`FanoutWorker` 和 `EmbeddingWorker`
  - [ ] 观察 Worker 如何处理系统的平滑关机信号 (`signal.NotifyContext`)
- [ ] **阶段 1 自测思考题**：
  > 1. 为什么 `Repository` 接口定义在 `domain` 层，而其 GORM 实现却在 `infra/persistence` 层？这符合什么设计原则？  
  > 2. 如果不启动 Redis 或 RabbitMQ，API 服务会直接崩溃吗？代码是如何进行可选降级处理的？

---

## 阶段 2：自底向上精读 6 大业务链路 (预计耗时: 2~3 天)

**目标**：由浅入深逐条链路追踪源码，吃透参数解析、业务编排、领域实体和数据库/缓存写入。

### 模块 A：账户与鉴权链路 (最适合用来熟悉分层模板)
- [ ] 阅读路由：`POST /api/users`、`POST /api/sessions`、`GET /api/users/me`
- [ ] 追踪代码：
  - [ ] [interfaces/http/account/handler.go](file:///c:/Users/rsty/Desktop/gofeed/GCFeed/apps/api/internal/interfaces/http/account/handler.go)（参数解析与密码强度校验）
  - [ ] [application/account/service.go](file:///c:/Users/rsty/Desktop/gofeed/GCFeed/apps/api/internal/application/account/service.go)（密码 bcrypt 哈希与 JWT 签发）
  - [ ] [domain/account/entity.go](file:///c:/Users/rsty/Desktop/gofeed/GCFeed/apps/api/internal/domain/account/entity.go)（用户实体定义）
  - [ ] [infra/persistence/account/gorm.go](file:///c:/Users/rsty/Desktop/gofeed/GCFeed/apps/api/internal/infra/persistence/account/gorm.go)（MySQL 读写）
  - [ ] [interfaces/http/middleware/auth.go](file:///c:/Users/rsty/Desktop/gofeed/GCFeed/apps/api/internal/interfaces/http/middleware/auth.go)（严格 JWT 认证与可选 JWT 认证的区别）

### 模块 B：视频发布链路 (主体表与统计表设计)
- [ ] 阅读路由：`POST /api/videos`
- [ ] 追踪代码：
  - [ ] [interfaces/http/video/handler.go](file:///c:/Users/rsty/Desktop/gofeed/GCFeed/apps/api/internal/interfaces/http/video/handler.go)
  - [ ] [application/video/service.go](file:///c:/Users/rsty/Desktop/gofeed/GCFeed/apps/api/internal/application/video/service.go)
  - [ ] [infra/persistence/video/gorm.go](file:///c:/Users/rsty/Desktop/gofeed/GCFeed/apps/api/internal/infra/persistence/video/gorm.go)
- [ ] 重点理解：
  - [ ] 为什么拆分为 `video`（主体）和 `video_stat`（计数）两张表？
  - [ ] 创建视频时如何通过事务同时初始化 `video_stat`？
  - [ ] 客户端传递的 `Idempotency-Key` 如何在数据库层面防止重复发布？
  - [ ] 视频发布完成后投递 `VideoPublishedEvent` 触发了哪些异步操作？

### 模块 C：Feed 流核心读取链路 (核心重点)
- [ ] 阅读路由：`GET /api/feed-items` 与 `POST /api/feed-queries`
- [ ] 追踪代码：
  - [ ] [interfaces/http/feed/handler.go](file:///c:/Users/rsty/Desktop/gofeed/GCFeed/apps/api/internal/interfaces/http/feed/handler.go)
  - [ ] [application/feed/service.go](file:///c:/Users/rsty/Desktop/gofeed/GCFeed/apps/api/internal/application/feed/service.go)（核心类 `Service` 与 `TimelineStrategy`）
  - [ ] [domain/feed/cursor.go](file:///c:/Users/rsty/Desktop/gofeed/GCFeed/apps/api/internal/domain/feed/cursor.go)（游标编码/解码）
- [ ] 重点理解：
  - [ ] **复合游标分页机制**：为什么使用 `published_at DESC, id DESC` 替代传统 `offset/limit`？
  - [ ] **ID-Detail 分离设计**：页缓存为什么只存 ID 和排序属性？
  - [ ] **批量组装函数**：查看 `assembleFeedItems`，看如何通过批量 MGET 组装卡片与计数并恢复原始排序。

### 模块 D：点赞/收藏与异步落库链路 (高并发削峰)
- [ ] 阅读路由：`PUT /api/videos/:videoId/like`、`DELETE /api/videos/:videoId/like`
- [ ] 追踪代码：
  - [ ] [interfaces/http/interaction/handler.go](file:///c:/Users/rsty/Desktop/gofeed/GCFeed/apps/api/internal/interfaces/http/interaction/handler.go)
  - [ ] [application/interaction/service.go](file:///c:/Users/rsty/Desktop/gofeed/GCFeed/apps/api/internal/application/interaction/service.go)
  - [ ] [infra/cache/feed_cache.go](file:///c:/Users/rsty/Desktop/gofeed/GCFeed/apps/api/internal/infra/cache/feed_cache.go)（互动状态与计数缓存）
  - [ ] [infra/mq/rabbitmq.go](file:///c:/Users/rsty/Desktop/gofeed/GCFeed/apps/api/internal/infra/mq/rabbitmq.go)（事件投递）
  - [ ] [application/interaction/worker.go](file:///c:/Users/rsty/Desktop/gofeed/GCFeed/apps/api/internal/application/interaction/worker.go)（`ActionWorker` 消费落库）
- [ ] 重点理解：
  - [ ] 用户点赞时，接口为何能实现毫秒级响应？（先写 Redis 状态并累加计数，再发 MQ）
  - [ ] Worker 消费消息并写 MySQL 时，如何依靠数据库唯一索引保障最终一致与幂等？

### 模块 E：热门榜单滑动窗口 (Hot Feed)
- [ ] 阅读路由：`GET /api/feed-items?scene=hot`
- [ ] 追踪代码：
  - [ ] [application/feed/service.go](file:///c:/Users/rsty/Desktop/gofeed/GCFeed/apps/api/internal/application/feed/service.go)（`HotStrategy`）
  - [ ] [infra/cache/feed_cache.go](file:///c:/Users/rsty/Desktop/gofeed/GCFeed/apps/api/internal/infra/cache/feed_cache.go)（`ListHotWindowPage`）
- [ ] 重点理解：
  - [ ] 热度加权规则：点赞(+3)、收藏(+4)、评论(+5)、取消动作扣减。
  - [ ] **Redis ZSET 分钟桶设计**：`feed:hot:minute:v1:{yyyyMMddHHmm}`。
  - [ ] 查询 1 小时榜单时，如何利用 `ZUNIONSTORE` 合并最近 60 个分钟桶？

### 模块 F：推拉结合关注流 (Following Feed)
- [ ] 阅读路由：`GET /api/feed-items?scene=following`
- [ ] 追踪代码：
  - [ ] [application/feed/service.go](file:///c:/Users/rsty/Desktop/gofeed/GCFeed/apps/api/internal/application/feed/service.go)（`FollowingStrategy`）
  - [ ] [application/video/worker.go](file:///c:/Users/rsty/Desktop/gofeed/GCFeed/apps/api/internal/application/video/worker.go)（`FanoutWorker`）
  - [ ] [application/relation/service.go](file:///c:/Users/rsty/Desktop/gofeed/GCFeed/apps/api/internal/application/relation/service.go)（关注关系与回填）
- [ ] 重点理解：
  - [ ] 什么是“读扩散（Pull）”与“写扩散（Push/Fanout）”？
  - [ ] 本项目如何设定粉丝阈值：小作者写入粉丝 Inbox，大 V 走作者 Outbox 拉取聚合？
  - [ ] 用户新关注一个作者时，近期历史视频是如何回填到用户收件箱的？

---

## 阶段 3：攻克高并发亮点与防御策略 (预计耗时: 1 天)

**目标**：掌握真实生产环境中应对缓存穿透、击穿、雪崩及消息丢失的核心手段。

- [ ] **3.1 缓存保护机制**
  - [ ] **Singleflight 并发合并**：在 [application/feed/service.go](file:///c:/Users/rsty/Desktop/gofeed/GCFeed/apps/api/internal/application/feed/service.go) 找到 `singleflight.Group`，理解同 Key 瞬时并发请求如何只让 1 个协程打到 MySQL
  - [ ] **TTL 随机抖动 (Jitter)**：查看 Feed 页缓存设置 TTL 时的随机偏移算法，理解如何防止缓存在同一秒集中失效（雪崩）
  - [ ] **缓存故障自动降级**：分析当 Redis 返回连接超时或异常时，Feed 接口如何静默降级为直查数据库保可用
- [ ] **3.2 消息队列可靠性闭环**
  - [ ] 查看 [infra/mq/rabbitmq.go](file:///c:/Users/rsty/Desktop/gofeed/GCFeed/apps/api/internal/infra/mq/rabbitmq.go)
  - [ ] 找到消费者中的手动 `Ack(false)` 与失败时的 `Nack(false, true)` 重回队列机制
  - [ ] 思考如果消息重复投递，Worker 如何通过业务唯一键确保幂等
- [ ] **3.3 媒体文件传输优化**
  - [ ] 查看 [test/upload_static_range_test.go](file:///c:/Users/rsty/Desktop/gofeed/GCFeed/apps/api/test/upload_static_range_test.go)
  - [ ] 理解 HTTP 206 Partial Content (Range 请求) 对浏览器端拖动进度条和分片预加载的意义
  - [ ] 查看 MP4 上传后的 faststart (将 moov 头部元数据移至文件头部，提升首帧起播速度)
- [ ] **阶段 3 自测思考题**：
  > 1. Singleflight 会阻塞并发请求，如果下游数据库查询超时，Singleflight 会产生什么连锁反应？如何设置上下文超时？  
  > 2. 为什么高频点赞不直接在 MySQL 执行 `UPDATE video_stat SET like_count = like_count + 1 WHERE video_id = ?`？行锁在高并发下会产生什么瓶颈？

---

## 阶段 4：性能压测与监控排查实战 (预计耗时: 1 天)

**目标**：用真实的压测数据验证系统吞吐量，熟练通过 Grafana 和 Prometheus 观测系统瓶颈。

- [ ] **4.1 压测工具准备**
  - [ ] 阅读 [docs/performance-testing.md](file:///c:/Users/rsty/Desktop/gofeed/GCFeed/docs/performance-testing.md)
  - [ ] 确保本地或容器内已安装 `k6`
- [ ] **4.2 执行最新时间线流 (Timeline) 压测**
  - [ ] 运行 20 并发、持续 60 秒测试：
    ```bash
    SCENE=timeline VUS=20 DURATION=60s THINK_TIME=1 k6 run ...
    ```
  - [ ] 记录输出的 `http_reqs/s` (QPS) 与 `http_req_duration p(95)` (P95 延迟)
- [ ] **4.3 执行热门流 (Hot) 压测**
  - [ ] 运行热榜压测，检验 Redis ZSET 滑动窗口合并的吞吐能力
- [ ] **4.4 结合 Grafana 面板实时观测**
  - [ ] 访问 `http://127.0.0.1:3000/d/gcfeed-overview/gcfeed-overview`
  - [ ] 重点观察以下指标的变化曲线：
    - `API QPS`
    - `API P95 Latency`
    - `Feed Cache Hit Rate`（缓存命中率）
    - `Worker Success Rate`
- [ ] **4.5 使用 PromQL 抓取底层指标**
  - [ ] 访问 Prometheus `http://127.0.0.1:9090`，执行查询：
    ```promql
    sum(rate(gcfeed_http_requests_total[5m])) by (route)
    histogram_quantile(0.95, sum(rate(gcfeed_http_request_duration_seconds_bucket[5m])) by (le, route))
    ```
- [ ] **阶段 4 验收产出**：
  - [ ] 能够完整向他人解释：“在 20 VU 并发下，Timeline 首页接口的 QPS 达到 XX，P95 延迟为 XX ms，缓存命中率保持在 XX%”。

---

## 阶段 5：动手魔改与工程实践演练 (预计耗时: 1~2 天)

**目标**：脱离“只看不写”，通过亲自动手扩充功能，检验是否真正掌握了分层与业务规范。

- [ ] **演练任务 1：为曝光与观看事件接入批量异步落库**
  - [ ] 现状：目前客户端通过 `POST /api/video-view-events` 上报播放和完播事件
  - [ ] 目标：模仿 `ActionWorker`，在 `application/exposure` 中实现一个批量收集缓冲区（例如：累积 50 条或每隔 1 秒批量写入 MySQL）
  - [ ] 编写对应的单元测试验证批量落库行为
- [ ] **演练任务 2：实现一个轻量的审核拦截逻辑**
  - [ ] 现状：目前发布的视频默认为公开状态 (`StatusPublished`)
  - [ ] 目标：
    1. 在 `domain/video` 中引入审核状态（如 `StatusReviewing`、`StatusRejected`）
    2. 发布视频时默认为待审核
    3. 在 `TimelineStrategy` 和 `HotStrategy` 中校验仅对审核通过的视频可见
  - [ ] 编写测试验证：未通过审核的视频不会出现在公共 Feed 流中
- [ ] **演练任务 3：编写一个新的 API 集成测试用例**
  - [ ] 参考 [apps/api/test/feed_api_test.go](file:///c:/Users/rsty/Desktop/gofeed/GCFeed/apps/api/test/feed_api_test.go)
  - [ ] 编写一个边界测试（例如：游标损坏传递非法 base64 字符串时，接口是否正确返回 400 且不发生 Panic）

---

## 阶段 6：面试复盘与知识体系转化 (预计耗时: 1 天)

**目标**：将项目设计亮点转化为自己的技术语言，能够从容应对技术面试中的深度考察。

- [ ] **6.1 吸收面试高频题库**
  - [ ] 细读 [docs/interview-questions.md](file:///c:/Users/rsty/Desktop/gofeed/GCFeed/docs/interview-questions.md)（800+ 行专业问答库）
  - [ ] 能够脱稿回答以下大厂高频问题：
    - [ ] “请介绍你项目中 Feed 流的分页方案，为什么不用 offset 分页？”
    - [ ] “Feed 卡片组装为什么要做 ID-Detail 分离？如果不分离会怎样？”
    - [ ] “高频点赞场景下，你们是如何保证数据一致性与写入性能的？”
    - [ ] “大 V 发布视频时，关注流推拉结合是怎么实现的？”
    - [ ] “热门榜单为什么要按分钟分桶？直接维护一个大 ZSET 有什么问题？”
    - [ ] “你的缓存是如何做防击穿、防雪崩和容灾降级的？”
- [ ] **6.2 提炼简历项目经验**
  - [ ] 阅读 [docs/resume-highlights.md](file:///c:/Users/rsty/Desktop/gofeed/GCFeed/docs/resume-highlights.md)
  - [ ] 结合自己实际动手的收获，定制一份符合自己求职档位的简历项目介绍版本
- [ ] **6.3 达成通关！**
  - [ ] 全部阶段勾选完毕，你已经完整掌握了短视频 Feed 系统在高并发、缓存、异步、分层和工程化方面的核心精髓！

