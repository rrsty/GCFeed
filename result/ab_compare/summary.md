# GCFeed 三项核心优化 A/B 压测对比报告

- **测试时间**：2026-11-27 22:45 ~ 22:58 (UTC+8)
- **被测基线**：基于提交 `a586148`（未改动任何生产源码，所有对照实现与压测脚本独立存放于 [`apps/api/cmd/abbench/`](../../apps/api/cmd/abbench/) 与 [`tests/k6/like_write_test.js`](../../tests/k6/like_write_test.js)）
- **测试环境与数据规模**：
  - 单机 Docker Compose 基础设施（MySQL 8.4 `127.0.0.1:3308/GCFeed`、Redis 7.4 `127.0.0.1:6381`、RabbitMQ 3.13 `127.0.0.1:5672`）
  - MySQL 数据量：**1,001 个账号（`test_user_1` ~ `test_user_1000`）、50,000 条已发布视频、50,000 条 `video_stat`**
  - 隔离策略：Go 基准与对照 HTTP 服务均使用 **Redis DB 1** 及独立 RabbitMQ 队列 `gcfeed.bench.action_changed`，不干扰线上默认 DB 0 与 `gcfeed-worker`；压测结束已清理全部测试数据并恢复 `video_id=49990` 原值。

---

## 一、专题 1：复合游标 `(published_at, id)` 代替传统 `OFFSET`

> 原始数据：[`suite1_cursor_vs_offset.json`](suite1_cursor_vs_offset.json)

### 1.1 梯度深度单 SQL 基准与 `EXPLAIN ANALYZE` 对比（`limit=11`）

| 分页深度 (`offset`) | `OFFSET` avg | `OFFSET` P95 | 复合游标 avg | 复合游标 P95 | 平均耗时加速比 | `OFFSET` 执行计划特征 (`EXPLAIN ANALYZE`) |
| :---: | :---: | :---: | :---: | :---: | :---: | :--- |
| `100` | 0.950 ms | 1.311 ms | 0.835 ms | 1.224 ms | **1.14x** | 索引范围扫描 `idx_video_timeline`（111 行） |
| `1,000` | 1.893 ms | 2.363 ms | 0.874 ms | 1.433 ms | **2.17x** | 索引范围扫描 `idx_video_timeline`（1,011 行） |
| `5,000` | 6.362 ms | 6.775 ms | 0.921 ms | 1.209 ms | **6.91x** | 反向扫描 `idx_status_published`（5,011 行） |
| `10,000` | 13.579 ms | 15.162 ms | 0.804 ms | 1.107 ms | **16.89x** | **退化为全表扫描 50,000 行 + Sort (Filesort)** |
| `20,000` | 13.668 ms | 14.364 ms | 0.839 ms | 1.123 ms | **16.29x** | **退化为全表扫描 50,000 行 + Sort (Filesort)** |
| `30,000` | 14.965 ms | 16.251 ms | 0.969 ms | 1.348 ms | **15.44x** | **退化为全表扫描 50,000 行 + Sort (Filesort)** |
| `45,000` | 14.920 ms | 15.924 ms | 0.869 ms | 1.270 ms | **17.17x** | **退化为全表扫描 50,000 行 + Sort (Filesort)** |

- **关键机制实证**（深度 `45,000` 的 `EXPLAIN ANALYZE`）：
  - `OFFSET 45000, 11`：优化器判定跳过 45,000 行回表成本过高，直接放弃 `idx_video_timeline`，触发 `Table scan on video (50,000 rows)` + `Sort: video.published_at DESC, video.id DESC (45,011 rows)`，MySQL 引擎层耗时 **`20.7 ms`**。
  - 复合游标 `(published_at < ? OR (published_at = ? AND id < ?))`：稳定命中 `idx_video_timeline` 范围扫描，仅扫描 **`11` 行**，MySQL 引擎层耗时 **`0.0457 ~ 0.0476 ms`**。

### 1.2 并发深分页回源压测（绕过 Page 缓存，随机深度 `20,000 ~ 44,800`，各 15s）

| 并发数 | 分页方式 | 成功查询数 | QPS | avg | P50 | P95 | P99 | 错误数 | 提升倍数 |
| :---: | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| `20` | `OFFSET` | 8,051 | 536.71 | 37.216 ms | 35.537 ms | 51.121 ms | 65.925 ms | 0 | 1.00x |
| `20` | **复合游标** | 118,775 | **7,918.02** | **2.524 ms** | 2.348 ms | **3.904 ms** | 5.025 ms | 0 | **QPS 14.75x / P95 降 92.4%** |
| `50` | `OFFSET` | 8,143 | 542.82 | 91.781 ms | 88.250 ms | 156.439 ms | 205.106 ms | 0 | 1.00x |
| `50` | **复合游标** | 156,111 | **10,405.82** | **4.801 ms** | 4.520 ms | **7.770 ms** | 10.054 ms | 0 | **QPS 19.17x / P95 降 95.0%** |

> 结论：`OFFSET` 在 20→50 并发时因全表扫 + 排序把 MySQL CPU 打满，QPS 卡死在 ~540、P95 翻 3 倍飙至 `156.44 ms`；复合游标在 50 并发下仍达 **10,405.82 QPS**，P95 仅 **`7.77 ms`**。

---

## 二、专题 2：ID-Detail 缓存分离 vs 整页大 JSON 缓存

> 原始数据：[`suite2_cache_separation.json`](suite2_cache_separation.json)

### 2.1 Redis 内存占用对比（200 个逻辑页 × 10 条/页，基于 500 热点视频池）

| 缓存架构 | Redis Key 数 | `MEMORY USAGE` 汇总 | `used_memory` 增量 | 单逻辑页分摊内存 | 内存节省 |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **整页大 JSON 缓存**（每页存完整 `FeedItem[]`） | 200 | 1,651,200 bytes (1.57 MB) | 1,738,240 bytes | 8,256.00 bytes | — |
| **ID-Detail 缓存分离**（200 轻量 Page + 500 Card + 500 Stat Hash） | 1,200 | **690,256 bytes** (0.66 MB) | **776,640 bytes** | **3,451.28 bytes** | **节省 58.20%** |

### 2.2 高频读写混合压测（50 读协程 + 10 写协程，20 热点页覆盖 200 视频，15s）

> - **整页缓存**：为保证点赞计数不严重过期，写后失效受影响的整页缓存，读 miss 后回源 MySQL 重建。
> - **ID-Detail 分离**：写协程通过 Lua 原地更新 `video:stat:v2:{id}`，Page 与 Card 全程不失效。

| 缓存架构 | 读 QPS | 读 avg | 读 P95 | 读 P99 | 写操作数 | 有效缓存命中率 | MySQL `Com_select` 增量 |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **整页大 JSON（写后失效）** | 3,362.92 | 14.835 ms | 38.338 ms | 53.432 ms | 25,092 | 49.54% (Page 25,021 hit / 25,484 miss) | **50,939 次** |
| **ID-Detail 缓存分离** | **7,156.96** (**2.13x**) | **6.978 ms** | **10.332 ms** (**降 73.1%**) | **12.279 ms** | 34,325 | **100.00%** (Page 107,434 / Detail 107,402) | **0 次** |

### 2.3 个性化多用户流跨用户复用对比（50 读协程，200 用户各 50 个动态组合，15s）

> 模拟 `Following` / `Recommend` 场景：每个用户的 Feed 排序组合不同，整页缓存仅约 10% 命中，其余请求需重建；ID-Detail 分离下 `Card` 与 `Stat` 在所有用户间共享。

| 缓存架构 | 读 QPS | 读 avg | 读 P95 | 读 P99 | Card/Stat 命中率 | MySQL `Com_select` 增量 |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| **整页大 JSON 缓存** | 7,796.54 | 6.408 ms | 31.369 ms | 53.533 ms | 91.40% | **20,122 次** |
| **ID-Detail 缓存分离** | **10,185.74** (**1.31x**) | **4.902 ms** | **7.620 ms** (**降 75.7%**) | **9.407 ms** | **100.00%** | **0 次** |

---

## 三、专题 3：高并发写链路优化前后（`WATCH`+16 分片 vs Redis Lua 单 Hash + 窗口批刷）

> 原始数据：[`suite3_write_microbench.json`](suite3_write_microbench.json)、[`k6_stages_meta.json`](k6_stages_meta.json) 及 `k6_*.json`

### 3.1 存储组件层微基准 A：Redis 写层（`WATCH` + 16 分片 vs Lua 单 Hash `video:stat:v2:{id}`）

针对热点视频 `video_id=49990` 执行高频点赞/取消状态切换：

| 场景 | 并发 | 实现方案 | QPS | avg | P95 | P99 | 冲突/失败数（率） | Redis 总命令数/次 | Key 数量 |
| :--- | :---: | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **多用户热点争抢** | 50 | 优化前 (`WATCH`+16分片) | 4,092.46 | 12.196 ms | 17.083 ms | 19.842 ms | 0 (0.00%) | 46.05 cmds/op | 68 |
| **多用户热点争抢** | 50 | **优化后 (Lua 单 Hash)** | **26,296.47** (**6.43x**) | **1.898 ms** | **3.404 ms** | **4.864 ms** | 0 (0.00%) | **12.00 cmds/op** | 51 |
| **多用户热点争抢** | 100 | 优化前 (`WATCH`+16分片) | 4,911.54 | 20.310 ms | 26.590 ms | 29.824 ms | 0 (0.00%) | 46.08 cmds/op | 118 |
| **多用户热点争抢** | 100 | **优化后 (Lua 单 Hash)** | **27,118.94** (**5.52x**) | **3.682 ms** | **5.650 ms** | **7.142 ms** | 0 (0.00%) | **12.00 cmds/op** | 101 |
| **多用户热点争抢** | 200 | 优化前 (`WATCH`+16分片) | 5,052.51 | 39.442 ms | 48.126 ms | 53.096 ms | 0 (0.00%) | 46.14 cmds/op | 218 |
| **多用户热点争抢** | 200 | **优化后 (Lua 单 Hash)** | **26,986.40** (**5.34x**) | **7.400 ms** | **10.594 ms** | **12.721 ms** | 0 (0.00%) | **12.01 cmds/op** | 201 |
| **同用户高频连点** | 50 | 优化前 (`WATCH`+16分片) | 403.87 | 9.979 ms | 13.940 ms | 17.261 ms | **34,987 (94.54%)** | 7.22 cmds/op | 4 |
| **同用户高频连点** | 50 | **优化后 (Lua 单 Hash)** | **26,034.23** (**64.46x**) | **1.917 ms** | **3.467 ms** | **4.983 ms** | **0 (0.00%)** | **11.00 cmds/op** | 2 |

> 注：优化前由于每次写入需刷新 16 个分片过期时间并 Pipeline 读取 16 分片再写 JSON，单次写入耗费 ~46 条 Redis 命令；同用户并发连点时 `WATCH` 乐观锁产生 **94.54%** 事务冲突失败。升级 Lua 单 Hash 后，命令数降至 **12 条/次（不含额外分片）**、P95 下降 **78%~80%**，同用户连点冲突率归零。

### 3.2 存储组件层微基准 B：Worker 异步落库层（逐条更新 vs `WithBatchStatFlush(200ms, 50)`）

向同一热点视频 `49990` 灌入 **1,500 条** 状态变更事件（1,000 用户点赞 + 500 用户取消点赞，预置 `status=2` 基线排除冷插间隙锁干扰），20 并发消费：

| Worker 模式 | 处理事件数 | 失败数 | 总耗时 | 消费吞吐 | `video_stat` 刷库次数 | MySQL `Com_update` | `Innodb_row_lock_waits` | 锁等待总耗时 | DB `like_count` / 活跃行一致性 |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **优化前：逐条同步更新 `video_stat`** | 1,500 | 0 | 5.3933 s | 278.12 ev/s | 1,500 次 | 3,000 | **1,498 次** | **95,086 ms** | `500 / 500` (一致) |
| **优化后：窗口批刷 (`200ms / 50条`)** | 1,500 | 0 | **0.9834 s** | **1,525.40 ev/s** (**5.48x**) | **30 次** (**降 98.0%**) | **1,530** (**降 49.0%**) | **0 次** | **0 ms** | `500 / 500` (一致) |

### 3.3 端到端 HTTP 接口 k6 压测（三阶段对照 + 在线 Docker 验证）

测试脚本：[`tests/k6/like_write_test.js`](../../tests/k6/like_write_test.js)（`USER_COUNT=100`，每个 VU 绑定固定账号对 `video_id=49990` 交替发起 `PUT/DELETE /api/videos/49990/like`，`DURATION=20s`，`THINK_TIME=0`；每次前清理 Redis DB 1 并预置 100 位用户 `status=2` 初始行、`like_count=0`）。

| 阶段 / 模式 | 并发 VU | 点赞/取消完成数 | 写接口速率 (`iterations/s`) | k6 总 `http_reqs/s` (含100登录) | avg 延迟 | P95 延迟 | 错误率 | MySQL `Com_update` | MySQL `Innodb_row_lock_waits` (锁等待 ms) | 排空后一致性 (`DB stat / active / Redis`) |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :--- |
| **A: 同步写 MySQL** (`sync`) | 50 | 5,231 | 202.76/s | 206.64/s | 189.19 ms | 233.21 ms | 0.00% | 10,462 | **5,230 次 (951,769 ms)** | `31 / 31 / -` |
| **A: 同步写 MySQL** (`sync`) | 100 | 5,359 | 207.17/s | 211.04/s | 369.48 ms | 636.37 ms | 0.00% | 10,718 | **5,358 次 (960,519 ms)** | `47 / 47 / -` |
| **B: `WATCH`+16分片+无批刷** | 50 | 7,534 | 292.61/s | 296.49/s | 131.85 ms | 165.40 ms | 0.00% | 12,348 | **0 次 (0 ms)** | `33 / 33 / -`（采样时 `qDepth=1349` 后完成） |
| **B: `WATCH`+16分片+无批刷** | 100 | 2,534 | 94.00/s | 97.71/s | 781.45 ms | 911.35 ms | 0.00% | 7,531 | **0 次 (0 ms)** | `66 / 66 / -` |
| **C: Lua 单 Hash + 批刷** | 50 | 2,538 | 97.87/s | 101.73/s | 384.63 ms | 438.59 ms | 0.00% | **2,719** | **0 次 (0 ms)** | `34 / 34 / 34` |
| **C: Lua 单 Hash + 批刷** | 100 | 2,470 | 90.25/s | 93.90/s | 796.68 ms | 902.24 ms | 0.00% | **2,654** | **0 次 (0 ms)** | `58* / 70 / 70`（*见下方说明） |
| **Docker 实盘 API** (`:8088`, 含站内信) | 50 | 2,197 | 85.56/s | 89.46/s | 441.62 ms | 532.80 ms | 0.00% | **1,743** | 31 次 (1 ms) | 排空 3s 后 **`7 / 7 / 7`**（全队列归 0） |

#### 3.3.1 关键工程洞察（为什么端到端 HTTP 异步模式没有达到 Redis Lua 自身的 26,000 QPS？）

本次端到端对照压测不仅验证了数据库行锁消除效果（**MySQL `video_stat` 行锁等待从阶段 A 的 `5,230~5,358` 次 / `~952~961s` 累计等待直接降至 `0`，`Com_update` 在阶段 C 相比阶段 A 下降约 75%**），同时也精准暴露出当前异步写链路在 HTTP 层的真实瓶颈：

1. **RabbitMQ 单连接发布锁 + 同步 Confirm 串行化**：
   [`apps/api/internal/infra/mq/rabbitmq.go`](../../apps/api/internal/infra/mq/rabbitmq.go#L155-L198) 中的 `publishJSON` 使用单把 `r.pubMu.Lock()` 保护单个 `publishChannel`，并在锁内同步调用 `confirmation.WaitContext(confirmCtx)` 等待 Broker 持久化确认（在当前 Windows/Docker 磁盘下平均每次 Confirm 耗时约 `7.5~8 ms`，50 并发排队即 ~`385 ms`，100 并发排队即 ~`790 ms`），直接把异步 HTTP 吞吐上限锁死在 **~90~130 req/s**（仅阶段 B 50 VU 期间因磁盘刷盘偶发低延迟跑到 296 req/s）。
2. **`setActionAsync` 仍同步查询 MySQL**：
   [`apps/api/internal/application/interaction/service.go`](../../apps/api/internal/application/interaction/service.go#L308) 每次点赞/取消仍先执行一次 `s.repo.GetVideoStat(ctx, videoID)`，使每次异步写仍携带 1 次 MySQL `JOIN` 查询。
3. **`stage_c_lua` 100 VU 瞬时快照说明**：
   压测结束瞬间 `/bench/stats` 先查 `queue_depth`（仅统计 RabbitMQ `Ready` 数，不含 Consumer `Qos(20)` 正在消费的 `Unacked` 消息）为 0 后立刻 `Flush`，随后最后几条在途 `Unacked` 消息才入缓冲，导致当时 `db_like=58` 而 `active_rows=70 / redis_v2=70`；而在 Docker 实盘验证等待 3 秒彻底排空后，**`db_like_count = 7`、`db_active_rows = 7`、`redis video:stat:v2:49990 like_count = 7`** 达到 100% 强一致，死信队列 `gcfeed.dlq` 为 0。
4. **冷插间隙锁并发隐患定位**：
   在早期 3B 压测调试中发现，`interaction_action` 唯一索引为 `(user_id, video_id, action_type)`，当多个新用户首次并发对同一尾部新视频执行 `SELECT ... FOR UPDATE` + `INSERT` 时，会因共同落入 supremum gap 触发 InnoDB 间隙锁死锁（Error 1213）。线上由 RabbitMQ 退避重试兜底，同步路径或大批量首次点赞可改为 `INSERT ... ON DUPLICATE KEY UPDATE` 彻底规避。

- **下一步写链路演进方向**：将 `RabbitMQ.publishJSON` 升级为 **Publish Channel Pool（多 Channel 并发发布）或本地批量/异步 Confirm**，并在 `setActionAsync` 中优先读取 Redis `video:stat:v2:{id}` 仅在缺失时回源 MySQL，即可把 HTTP 端到端写吞吐从百级释放到千~万级。

---

## 四、原始数据文件清单

- Go 基准 JSON：
  - [`suite1_cursor_vs_offset.json`](suite1_cursor_vs_offset.json)
  - [`suite2_cache_separation.json`](suite2_cache_separation.json)
  - [`suite3_write_microbench.json`](suite3_write_microbench.json)
- k6 HTTP 端到端压测原始输出：
  - [`k6_stages_meta.json`](k6_stages_meta.json)
  - [`k6_stage_a_sync_50vu.txt`](k6_stage_a_sync_50vu.txt) / [`k6_stage_a_sync_50vu.json`](k6_stage_a_sync_50vu.json)
  - [`k6_stage_a_sync_100vu.txt`](k6_stage_a_sync_100vu.txt) / [`k6_stage_a_sync_100vu.json`](k6_stage_a_sync_100vu.json)
  - [`k6_stage_b_watch_50vu.txt`](k6_stage_b_watch_50vu.txt) / [`k6_stage_b_watch_50vu.json`](k6_stage_b_watch_50vu.json)
  - [`k6_stage_b_watch_100vu.txt`](k6_stage_b_watch_100vu.txt) / [`k6_stage_b_watch_100vu.json`](k6_stage_b_watch_100vu.json)
  - [`k6_stage_c_lua_50vu.txt`](k6_stage_c_lua_50vu.txt) / [`k6_stage_c_lua_50vu.json`](k6_stage_c_lua_50vu.json)
  - [`k6_stage_c_lua_100vu.txt`](k6_stage_c_lua_100vu.txt) / [`k6_stage_c_lua_100vu.json`](k6_stage_c_lua_100vu.json)
  - [`k6_docker_api_50vu.txt`](k6_docker_api_50vu.txt) / [`k6_docker_api_50vu.json`](k6_docker_api_50vu.json)
