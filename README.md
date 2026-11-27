# GCFeed — 高并发短视频 Feed 流系统

GCFeed 是一个面向短视频高并发分发、互动与消费场景的工业级 Feed 流工程系统。基于 **Go 1.23+、Gin、MySQL 8.4、Redis 7.4、RabbitMQ 3.13 与 React 18** 构建，具备分层领域单体架构、推拉结合关注流、实时热榜、向量兴趣推荐流、Redis Lua 原子计数与窗口聚合批刷削峰落库及 Prometheus + Grafana 全景监控能力。

本项目已历经多轮生产级架构审查，完成了涵盖 **安全访问控制、Redis Lua 单 Hash 互动计数升级、MySQL 防死锁与索引治理、RabbitMQ 可靠投递与 DLQ、缓存降级闭环及服务优雅停机** 的系统性演进。

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
- [性能压测结果（50,000 视频规模）](#性能压测结果50000-视频规模)
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

3. **高并发互动与计数削峰（Route A）**：
   - **Redis Lua 单 Hash 原子计数**：视频点赞、收藏与评论计数收口至单 Hash `video:stat:v2:{id}`，通过 Lua 脚本在一次原子执行中完成用户状态切换、非负约束校验与计数增减，避免传统 `WATCH` 乐观锁冲突。
   - **窗口聚合批刷与乱序保护**：变更事件经 RabbitMQ（Publisher Confirm + DLQ 死信保障）投递至 Worker，按时间与数量窗口（`200ms / 50` 条）合并同一视频增量后，通过原子 SQL（`GREATEST(0, col + ?)`）批量落库 MySQL，并配合纳秒级 `last_event_at_ns` 乱序保护与 Read-Your-Own-Writes 读写一致。

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
│  ├─ Recommend (Vector + Freshness + Exposure Filter)       ├─ Lua      │
│  ├─ Following (Push-Pull Hybrid ZSET)                      │  Atomic   │
│  └─ Hot (Dynamic Score Window)                             └─ Cache    │
└───────────────────┬───────────────────────────────┬────────────────────┘
                    │                               │
         Read / Sync State                Async Events (Confirm)
                    │                               │
       ┌────────────▼────────────┐             ┌────▼────────────────────┐
       │        Redis 7.4        │             │   RabbitMQ 3.13 (Broker)│
       │ ├─ Feed Cards & Stats   │             │ ├─ Video Published Topic│
       │ ├─ Following In/Outbox  │             │ ├─ Action Changed Queue │
       │ └─ Single-Hash Stats v2 │             │ └─ Retry + DLX / DLQ    │
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
- **Redis 状态缺失时行为纠偏**：持久层补充 `GetAction`。在点赞/取消点赞或收藏时若 Redis 缓存淘汰，先回源数据库恢复用户真实状态，确保计算准确增量并同步 MQ 投递，杜绝数据库与缓存永久失步。
- **Redis Lua 单 Hash 互动计数与 Worker 批刷（Route A）**：将原 `WATCH` + 16 分片计数升级为单 Hash `video:stat:v2:{id}` + Lua 原子脚本；Worker 通过 `WithBatchStatFlush(200ms, 50)` 按窗口合并增量后以原子 SQL `GREATEST(0, col + ?)` 批量落库，大幅降低热点视频行锁与 DB 写入频次。
- **互动消费纳秒级乱序版本保护**：`SetAction` / `SetActionRecordOnly` 引入 `last_event_at_ns` 纳秒时间戳校验，乱序或迟到消息安全跳过，防止旧状态覆盖新状态。
- **MySQL 防死锁、索引与慢查询治理**：移除 `account` / `video` 热点 `FOR UPDATE` 行锁，互关按 `user_id` 升序加锁消除 AB-BA 死锁；消除个人主页全表 `GROUP BY` 派生子查询；补齐 `message`、`exposure`、`interaction`、`video` 联合索引及连接池生命周期配置。
- **RabbitMQ 并发安全、Confirm 与 DLQ 闭环**：消费者分配独立 Channel 与 `Qos(20)`；Publish 加锁并启用 Publisher Confirm；接入独立 `gcfeed.dlx` / `gcfeed.dlq` 死信队列与有限次精准重试、断线指数退避重连，并在 API 与 Worker 接入 `signal.NotifyContext` 优雅停机。
- **关注流校验与缓存降级兜底**：修复关注流收件箱误用大 V 列表校验普通关注作者导致的漏删/误删，改为全量关注作者校验 + 批量异步 `ZRem`；补齐热门流/关注流冷启动与 Redis 异常 DB 降级、`singleflight` 脱离请求取消上下文、前端播放完播埋点及 Prometheus 路由防高基数爆炸。
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

# 构建并启动全部容器（MySQL、Redis、RabbitMQ、自动数据初始化 Seed、API、Worker、Web、Prometheus、Grafana）
docker compose up -d --build
```

> **自动数据填充**：容器编排已内置 `seed` 初始化任务。在 MySQL 健康检查通过后，会自动注入 20 个测试创作者、100 条带封面/播放流的测试视频、特征向量及点赞/关注数据，服务启动完毕后即可开箱即用。

#### 服务入口映射

| 服务名称 | 访问地址 | 说明 / 默认凭据 |
| :--- | :--- | :--- |
| **Web 客户端** | `http://127.0.0.1:5173` | 短视频浏览、播放与互动前端 |
| **API 健康探针** | `http://127.0.0.1:8088/health` | 状态探活检查 |
| **API 指标端点** | `http://127.0.0.1:8088/metrics` | Prometheus 采集入口 |
| **MySQL 数据库** | `127.0.0.1:3308` | 用户: `root` / 密码: `123456` |
| **Redis 缓存** | `127.0.0.1:6381` | 端口直连 |
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

## 性能压测结果（50,000 视频规模）

基于 [tests/k6/load_test.js](tests/k6/load_test.js) 在单机 Docker Compose 全栈环境（Windows / Intel i7-12700H 14C20T / 15.7 GB RAM）下执行压测；MySQL 业务数据规模为 **1,000 个用户、50,000 条已发布视频、50,000 条特征向量、20,000 条关注关系**。

| 场景 | 模式 | 并发 VU / 思考时间 | 总请求数 | QPS | 平均延迟 (avg) | P95 延迟 | HTTP 错误率 | 阈值校验 |
| :--- | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **Timeline** | 标准模拟 | 20 VU / 1s | 600 | 19.85 | 5.97 ms | 11.77 ms | 0.00% | PASS |
| **Hot** | 标准模拟 | 20 VU / 1s | 600 | 19.87 | 4.60 ms | 8.95 ms | 0.00% | PASS |
| **Following** | 标准模拟 | 20 VU / 1s | 601 | 19.66 | 15.61 ms | 32.71 ms | 0.00% | PASS |
| **Recommend** | 标准模拟 | 20 VU / 1s | 516 | 16.56 | 187.59 ms | 309.80 ms | 0.00% | PASS |
| **Timeline** | 极限吞吐 | 50 VU / 0s | 202,971 | **6,764.11** | 7.07 ms | 11.62 ms | 0.00% | PASS |
| **Timeline** | 极限吞吐 | 100 VU / 0s | 207,852 | **6,925.86** | 14.02 ms | 25.34 ms | 0.00% | PASS |
| **Timeline** | 极限吞吐 | 200 VU / 0s | 233,360 | **7,773.23** | 25.27 ms | 44.81 ms | 0.00% | PASS |
| **Hot** | 极限吞吐 | 50 VU / 0s | 189,188 | **6,304.43** | 7.57 ms | 12.07 ms | 0.00% | PASS |
| **Following** | 极限吞吐 | 50 VU / 0s | 44,513 | **1,479.99** | 33.36 ms | 55.31 ms | 0.00% | PASS |
| **Recommend** | 极限吞吐 | 50 VU / 0s | 1,658 | 54.30 | 913.80 ms | 1,565.61 ms | 0.00% | `p95>500ms` |

- **多级缓存命中率**（Prometheus 累计增量）：Page 页缓存 **99.82%**（643,666 hit / 1,147 miss）、Card 卡片缓存 **100.00%**（8,818,550 hit）、Stat 计数缓存 **99.996%**（8,818,240 hit / 310 miss）。
- **5 万行深分页实测**（`EXPLAIN` 命中 `idx_video_timeline`）：第 45,000 条之后取 10 条，复合游标查询耗时 **`0.84 ms`**，对比传统 `LIMIT 45000, 10`（`23.88 ms`）提升约 **28.3 倍**。
- **瓶颈与演进定位**：`Recommend` 在 50 VU 极限压测下 P95 升至 `1,565.61 ms`（0% 错误率），由慢日志定位为 `ListCandidatePool` 对 5 万行执行动态 `hot_score` 排序产生的 Filesort（单 SQL 260~310 ms），后续可通过将 Top 500 候选池预同步至 Redis ZSET 或物化带索引热度列优化。
- **完整报告与原始数据**：见 [result/summary.md](result/summary.md) 及 [`result/`](result/) 下的原始 k6 JSON/文本输出与 Prometheus `/metrics` 快照。

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
| **[result/summary.md](result/summary.md)** | **50,000 视频规模 k6 压测汇总报告与原始数据** |

---

## 鸣谢与开源致谢

本项目原 Fork 自开源工程：
- **上游仓库源**：[https://github.com/LeoninCS/GCFeed](https://github.com/LeoninCS/GCFeed)
- **原始作者**：[LeoninCS](https://github.com/LeoninCS)

感谢原作者对于 Feed 流架构原型与基础单体分层工程的探索与开源贡献。本项目在此基础上进行了全面的 **P0/P1 工业级安全加固、跨存储状态一致性修复、事件版本防护、生命周期管理与端到端防竞态演进**。
