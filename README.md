# GCFeed — 高并发短视频 Feed 流系统

GCFeed 是一个面向短视频高并发分发、互动与消费场景的工业级 Feed 流工程系统。基于 **Go 1.23+、Gin、MySQL 8.4、Redis 7.4、RabbitMQ 3.13 与 React 18** 构建，具备分层领域单体架构、推拉结合关注流、实时热榜、向量兴趣推荐流、分片计数削峰落库及 Prometheus + Grafana 全景监控能力。

本项目已历经生产级架构审查，完成了涵盖 **安全访问控制、跨存储数据一致性、分布式消费版本保护、缓存失效闭环与前端翻页防竞态** 的系统性优化。

---

## 目录

- [系统特性与架构亮点](#系统特性与架构亮点)
- [核心业务与技术架构](#核心业务与技术架构)
- [架构演进与优化实施专题 (2026-11)](#架构演进与优化实施专题-2026-11)
  - [P0 级访问控制与同源防护](#1-p0-级访问控制与同源防护)
  - [P1 级数据一致性与消费可靠性闭环](#2-p1-级数据一致性与消费可靠性闭环)
- [快速开始](#快速开始)
  - [使用 Docker Compose 一键启动](#1-使用-docker-compose-一键启动)
  - [本地开发环境搭建](#2-本地开发环境搭建)
- [可观测性与监控面板](#可观测性与监控面板)
- [验证与质量保障](#验证与质量保障)
- [文档地图](#文档地图)
- [鸣谢与开源致谢](#鸣谢与开源致谢)

---

## 系统特性与架构亮点

1. **清晰的领域分层架构（DDD-inspired）**：
   - **Domain**：聚合根、实体状态机与领域错误，无第三方依赖；
   - **Application**：用例编排、Feed 策略聚合、事件发布与异步管道；
   - **Infrastructure**：MySQL (GORM) 持久化、Redis 分片与多级缓存、RabbitMQ 拓扑；
   - **Interfaces**：RESTful API 契约、分层参数校验与统一安全中间件。

2. **多场景 Feed 混合分发体系**：
   - **关注流（Following）**：采用**推拉结合（Push-Pull）**策略。粉丝数小于阈值（大 V 判定）的作者发视频写粉丝收件箱（Inbox）；粉丝数超限的大 V 作者发视频只写发件箱（Outbox），用户下拉刷新时由服务端并发拉取合并，解决大 V 写入扇出风暴与普通创作者读延迟。
   - **热门流（Hot）**：按点赞、评论、收藏与完播率计算加权分数，配合时间衰减因子生成实时分数，利用 Redis ZSET 滑动窗口与 MySQL 进行兜底稳定分页。
   - **推荐流（Recommend）**：基于用户观看历史实时构建兴趣向量，结合视频 N-Gram 文本特征向量进行**余弦相似度计算 + 新鲜度加权 + 近期曝光过滤去重**，提供打散重排候选。

3. **高并发互动与计数削峰**：
   - **分片计数器（Counter Sharding）**：视频点赞/收藏计数拆分为多个独立 Shard Key 并行递增/递减，大幅减轻单一 Redis 热点 Key 访问争用。
   - **异步缓冲与落库闭环**：快速状态先落 Redis 并通过 Singleflight 与轻量 JSON 缓存支持超低时延读取；变更事件发往 RabbitMQ 削峰异步持久化至 MySQL。

4. **端到端生产级稳定性**：
   - **防击穿、防雪崩**：核心卡片查询接入 `singleflight` 抑制并发回源，缓存过期时间加入动态随机抖动（Jitter）。
   - **游标深度分页**：弃用传统 `OFFSET` 分页，全链路统一采用 `(score/timestamp, id)` 复合游标，杜绝 MySQL 深度分页慢查询与并发数据漂移。

---

## 核心业务与技术架构

```text
┌────────────────────────────────────────────────────────────────────────┐
│                          React + Vite Web App                          │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │ HTTP / REST / Auth
┌───────────────────────────────────▼────────────────────────────────────┐
│                       GCFeed API Gateway (Gin)                         │
│  ┌──────────────────┬──────────────────┬────────────────────────────┐  │
│  │ JWT / Internal   │ Security Headers │ Metrics & Rate Limiter     │  │
│  │ Auth Middleware  │ & CSP Guards     │ (Prometheus Instrumentation│  │
│  └──────────────────┴──────────────────┴────────────────────────────┘  │
│                                   │                                    │
│   ┌───────────────────────────────┴───────────────────────────────┐    │
│   ▼                                                               ▼    │
│ [Feed Service]                                            [Interaction]│
│  ├─ Recommend (Vector + Freshness + Exposure Filter)       ├─ Sharded  │
│  ├─ Following (Push-Pull Hybrid ZSET)                      │  Counter  │
│  └─ Hot (Dynamic Score Window)                             └─ Cache    │
└───────────────────┬───────────────────────────────┬────────────────────┘
                    │                               │
         Read / Sync State                Async Events
                    │                               │
       ┌────────────▼────────────┐             ┌────▼────────────────────┐
       │   Redis 7.4 (Cluster)   │             │   RabbitMQ 3.13 (Broker)│
       │ ├─ Feed Cards & Stats   │             │ ├─ Video Published Topic│
       │ ├─ Following In/Outbox  │             │ ├─ Action Changed Queue │
       │ └─ Sharded Action Stats │             │ └─ View Event Recorded  │
       └────────────┬────────────┘             └────┬────────────────────┘
                    │                               │
         Cache Miss / Batch Load             Worker Consumption
                    │                               │
       ┌────────────▼───────────────────────────────▼────────────────────┐
       │                       MySQL 8.4 Database                        │
       │   Video (Published/Status) │ Video Stat │ User Relation │ Action│
       └─────────────────────────────────────────────────────────────────┘
```

---

## 架构演进与优化实施专题 (2026-11)

针对系统深入审查（见 [docs/optimization-audit-2026-11-03.md](docs/optimization-audit-2026-11-03.md)），本项目对所有 **P0 严重级安全与阻断隐患** 及 **12 项 P1 级跨存储一致性、故障恢复性与竞态问题** 完成了完整闭环演进（详见完整报告：[docs/optimization-implementation-report-2026-11-03.md](docs/optimization-implementation-report-2026-11-03.md)）：

### 1. P0 级访问控制与同源防护
- **内部接口统一强鉴权**：`/internal/*`（推荐候选、曝光决策与曝光持久化等）全部纳入统一 `InternalTokenAuth` 中间件，彻底防御匿名调用者伪造其他用户曝光记录或遍历内部推荐候选。
- **上传主动脚本与同源隔离**：废除通用的 `"file"` 上传类型，仅允许受限的业务资源类型（`video`、`cover`、`avatar`）；对所有类型强制执行扩展名白名单与二进制头部嗅探（`sniffUploadMIME`），禁止 HTML/SVG 等主动内容上传。
- **生产环境默认弱凭据阻断**：支持 `JWT_SECRET`、`INTERNAL_TOKEN`、`MYSQL_PASSWORD` 等环境变量动态注入；生产模式（`APP_ENV=production`）下强制检测，命中示例弱口令直接拒绝进程启动，杜绝离线伪造任意用户身份。
- **静态资源安全加固**：对静态托管路由注入 `X-Content-Type-Options: nosniff` 与严格 CSP 策略，严禁目录浏览遍历。

### 2. P1 级数据一致性与消费可靠性闭环
- **Redis 状态缺失时行为纠偏**：持久层补充 `GetAction`。在取消点赞/收藏时若 Redis 缓存淘汰，先回源数据库恢复用户真实状态，确保取消操作能准确计算出 `delta = -1` 并触发 MQ 投递，杜绝数据库与缓存永久失步。
- **分片计数 TTL 生命周期强同步**：在分片计数管道写入中，遍历并同步刷新 Base Key 与所有 Shard Keys 的 TTL，根除因冷分片提前过期导致视频前台计数断崖下跌的 Bug。
- **互动消费单调时间戳版本保护**：`SetAction` 持久化操作引入 `OccurredAt` 校验，若数据库已有状态更新时间晚于事件发生时间，直接作为迟到消息安全跳过，防止网络乱序或重投导致旧点赞覆盖较新的取消状态。
- **MQ 拓扑限流与观看消费者闭环**：消费者通道配置 `Qos(20)` 预取上限，防止消息堆积打爆 Worker 内存；消费异常重投引入 50ms 延迟退避防止 CPU 忙轮询；在 Worker 启动中接入 `ConsumeViewEventRecorded` 观看队列消费者，根除消息无端积压。
- **关注流取关残留过滤与惰性清理**：关注流拉取时对 inbox 与 outbox 统一做作者关注关系校验；取关后旧作者内容自动过滤，并在后台异步执行 `ZRem` 清除用户 inbox 残留项。
- **关注流索引完整性回源兜底**：当 Redis 索引命中条数不足 `limit` 时（因截断或大 V 边界迁移），自动回源数据库 `ListFollowingPage` 兜底，保障翻页完整性。
- **关注流游标全序一致过滤**：在应用层按 `(PublishedAt, VideoID)` 元组对游标进行全序精确过滤，消除同秒取模倒挂与浮点精度边缘导致的漏项/重复项。
- **视频删除缓存失效闭环**：`videoService.Delete` 软删除成功后即刻调用 `DeleteCard` 失效 Feed 卡片缓存；由于数据库只查发布态视频，被删除视频实现全局即时下架。
- **推荐分页会话基准时间锁定**：推荐游标携带 `SessionTime`，跨页翻页时计算新鲜度与候选打分基准时间恒定，防止跨分钟候选排序微变引发的跳项或重复推荐。
- **前端翻页请求场景守卫**：在 `App.jsx` 中为翻页请求增加 `feedSceneRef` 守卫，在响应返回落地及 finally 清理加载态前核验当前活动场景，彻底根除切换 Tab 时的翻页串场竞态。

---

## 快速开始

### 1. 使用 Docker Compose 一键启动

**前置依赖**：Docker 24+ 与 Docker Compose v2+。

```bash
# 进入工程应用目录
cd apps

# 构建并启动全部容器（MySQL、Redis、RabbitMQ、API、Worker、Web、Prometheus、Grafana）
docker compose up -d --build
```

#### 服务入口映射

| 服务名称 | 访问地址 | 说明 / 默认凭据 |
| :--- | :--- | :--- |
| **Web 客户端** | `http://127.0.0.1:5173` | 短视频浏览、播放与互动前端 |
| **API 健康探针** | `http://127.0.0.1:8080/health` | 状态探活检查 |
| **API 指标端点** | `http://127.0.0.1:8080/metrics` | Prometheus 采集入口 |
| **MySQL 数据库** | `127.0.0.1:3307` | 用户: `root` / 密码: `123456` |
| **Redis 缓存** | `127.0.0.1:6379` | 端口直连 |
| **RabbitMQ 管理台** | `http://127.0.0.1:15672` | 账号: `guest` / 密码: `guest` |
| **Prometheus** | `http://127.0.0.1:9090` | 监控指标查询 |
| **Grafana Dashboard**| `http://127.0.0.1:3000` | 账号: `admin` / 密码: `admin` |

#### 查看日志与停止

```bash
# 跟踪核心 API 与 Worker 日志
docker compose logs -f api worker

# 停止容器服务
docker compose down

# 停止并彻底清理数据卷（包含数据库、Redis 与上传文件）
docker compose down -v
```

---

### 2. 本地开发环境搭建

若要在本地分别运行微服务与客户端：

#### 启动后端依赖中间件
确保本地已安装运行 MySQL、Redis 与 RabbitMQ，并配置 `apps/api/configs/config.yaml`。

#### 运行 API 服务与 Worker
```bash
# 启动 API HTTP 主服务
cd apps/api
go run cmd/feed/main.go

# 新开终端启动异步 Worker（分发、向量计算、互动落库、观看消费）
cd apps/api
go run cmd/worker/main.go
```

#### 运行前端 Web 客户端
```bash
cd apps/web
npm install
npm run dev
```

---

## 可观测性与监控面板

系统内置开箱即用的 Prometheus 采集与 Grafana 监控大盘，访问：
`http://127.0.0.1:3000/d/gcfeed-overview/gcfeed-overview`

**面板核心监控维度**：
1. **API 层**：全站请求 QPS、HTTP 状态码分布（5xx / 4xx 错误率）、API P95/P99 耗时分位值。
2. **Feed 流引擎**：各场景（Recommend / Hot / Following）P95 时延、各级 Feed 缓存命中率。
3. **异步消费链路**：RabbitMQ 各队列深度、Worker 消费时延、消费成功/重试率。
4. **多媒体与存储**：视频转封装（Faststart）处理耗时分布、数据库与 Redis 往返频率。

---

## 验证与质量保障

全项目均配套完备的自动化测试与类型检查套件，合并前执行以下校验：

```bash
# 1. 后端自动化单元测试与 API 流程集成测试（包含新增的安全与版本测试）
cd apps/api
go test -v ./...

# 2. 前端生产环境构建检查（确保语法、无死锁与 Vite 编译通过）
cd apps/web
npm run build

# 3. Docker Compose 编排语法校验
cd apps
docker compose config
```

---

## 文档地图

| 文档路径 | 核心内容 |
| :--- | :--- |
| **[docs/optimization-implementation-report-2026-11-03.md](docs/optimization-implementation-report-2026-11-03.md)** | **P0/P1 问题优化与排查实施全景报告（本期重点）** |
| [docs/optimization-audit-2026-11-03.md](docs/optimization-audit-2026-11-03.md) | 全项目系统审查审计原始报告 |
| [docs/project-analysis.md](docs/project-analysis.md) | 项目深度拆解与系统学习路径指南 |
| [docs/learning-checklist.md](docs/learning-checklist.md) | 源码研读通关清单与自测点 |
| [docs/architecture.md](docs/architecture.md) | 核心架构、领域分层模型、推拉算法与数据库 ER 图 |
| [docs/engineering.md](docs/engineering.md) | 编码规约、分层调用边界、目录规范与错误码规范 |
| [docs/performance-testing.md](docs/performance-testing.md) | k6 压测指南、压测脚本与指标调优分析 |
| [docs/resume-highlights.md](docs/resume-highlights.md) | 简历项目亮点提取与技术问答梳理 |

---

## 鸣谢与开源致谢

本项目原 Fork 自开源工程：
- **上游仓库源**：[https://github.com/LeoninCS/GCFeed](https://github.com/LeoninCS/GCFeed)
- **原始作者**：[LeoninCS](https://github.com/LeoninCS)

感谢原作者对于 Feed 流架构原型与基础单体分层工程的探索与开源贡献。本项目在此基础上进行了全面的 **P0/P1 工业级安全加固、跨存储状态一致性修复、事件版本防护、生命周期管理与端到端防竞态演进**。
