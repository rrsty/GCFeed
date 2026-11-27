# GCFeed k6 全量压测结果汇总报告（50,000 视频规模）

- **测试时间**：2026-11-27 16:49 ~ 16:55 (UTC+8)
- **被测版本**：基于最新提交 `4e0af08`（同时完善 `apps/api/cmd/seed/main.go` 批量写入以支持 5 万条规模数据注入）
- **压测脚本**：[`tests/k6/load_test.js`](../tests/k6/load_test.js)
- **压测目标**：`http://127.0.0.1:8088`（单机 Docker Compose 全栈：`gcfeed-api` + `gcfeed-mysql` + `gcfeed-redis` + `gcfeed-rabbitmq` + `gcfeed-worker` + `gcfeed-prometheus`）

---

## 一、测试环境与数据规模

### 1. 宿主机硬件环境
- **操作系统**：Microsoft Windows NT 10.0.26200.0
- **CPU**：12th Gen Intel(R) Core(TM) i7-12700H（14 核 / 20 逻辑处理器）
- **内存**：15.7 GB

### 2. MySQL 业务数据规模（实测行数，较基线扩容 500 倍）
| 数据表 | 行数 | 说明 |
| :--- | :---: | :--- |
| `account` | 1,000 | `test_user_1` ~ `test_user_1000` |
| `video` | 50,000 | 已发布视频（发布时间按分钟递增分布） |
| `video_stat` | 50,000 | 对应统计记录（含梯度热度） |
| `user_follow` | 20,000 | 每位用户关注 20 位创作者 |
| `video_embedding` | 50,000 | 128 维文本哈希特征向量 |

### 3. 压测前预热与 MySQL 索引 / 深分页实测
1. 清空旧 Redis (`FLUSHDB`) 并重启 `api` / `worker`（RabbitMQ 连接正常），登录 `test_user_1` 对最新视频 `49991..50000` 发起 10 次点赞，填充 Redis 60 分钟热榜窗口桶与单 Hash 计数缓存。
2. 冒烟验证四类场景均返回 HTTP 200 且返回条数为 **满页 10 条**（`timeline=10, hot=10, following=10, recommend=10`），并采集压测前基线 [`metrics_before.txt`](metrics_before.txt)。
3. **50,000 行 MySQL `EXPLAIN` 与深分页实测**：
   - `EXPLAIN SELECT id, author_id, published_at FROM video WHERE status = 2 AND published_at IS NOT NULL ORDER BY published_at DESC, id DESC LIMIT 11;`
     命中联合索引 `idx_video_timeline`（`type=range`，`key_len=9`，`Extra=Using index condition`），无 `Using filesort`。
   - 深分页对比（第 45,000 条之后取 10 条，`SHOW PROFILES` 实测）：
     - 传统 `LIMIT 45000, 10`：**`23.88 ms`**（`0.02388325 s`）
     - 复合游标 `(published_at < @c_pub OR (published_at = @c_pub AND id < @c_id)) LIMIT 10`：**`0.84 ms`**（`0.00084300 s`，快约 **28.3 倍**）

---

## 二、标准业务场景模拟（Group A：`VUS=20`，`DURATION=30s`，`THINK_TIME=1s`）

> 模拟真实用户每次浏览停顿 1 秒的稳定请求模型（单次响应 `limit=10`）。

| 场景 | 并发 VU | 时长 | 总请求数 | QPS (`http_reqs/s`) | 平均延迟 (avg) | 中位延迟 (med) | P90 延迟 | P95 延迟 | 最大延迟 (max) | HTTP 错误率 | 业务成功率 | 阈值校验 (`p95<500ms, err<1%, ok>99%`) | 原始文件 |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :--- |
| **Timeline（时间线）** | 20 | 30s | 600 | 19.85 | 5.97 ms | 5.92 ms | 10.00 ms | 11.77 ms | 15.55 ms | 0.00% | 100.00% | PASS (`exit=0`) | [`01_timeline_standard.txt`](01_timeline_standard.txt) / [JSON](01_timeline_standard.json) |
| **Hot（热门榜）** | 20 | 30s | 600 | 19.87 | 4.60 ms | 4.39 ms | 7.76 ms | 8.95 ms | 18.26 ms | 0.00% | 100.00% | PASS (`exit=0`) | [`02_hot_standard.txt`](02_hot_standard.txt) / [JSON](02_hot_standard.json) |
| **Following（关注流）** | 20 | 30s | 601 | 19.66 | 15.61 ms | 13.68 ms | 26.81 ms | 32.71 ms | 53.50 ms | 0.00% | 100.00% | PASS (`exit=0`) | [`03_following_standard.txt`](03_following_standard.txt) / [JSON](03_following_standard.json) |
| **Recommend（推荐流）** | 20 | 30s | 516 | 16.56 | 187.59 ms | 189.30 ms | 290.61 ms | 309.80 ms | 339.89 ms | 0.00% | 100.00% | PASS (`exit=0`) | [`04_recommend_standard.txt`](04_recommend_standard.txt) / [JSON](04_recommend_standard.json) |

---

## 三、极限吞吐压测（Group B：`THINK_TIME=0s`，`DURATION=30s`）

> 取消用户思考时间，压测 5 万视频规模下单机 API 实例的吞吐极限与慢查询暴露情况。

| 场景 | 并发 VU | 时长 | 总请求数 | QPS (`http_reqs/s`) | 平均延迟 (avg) | 中位延迟 (med) | P90 延迟 | P95 延迟 | 最大延迟 (max) | HTTP 错误率 | 业务成功率 | 阈值校验 | 原始文件 |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :--- |
| **Timeline 50 VU** | 50 | 30s | 202,971 | **6,764.11** | 7.07 ms | 6.71 ms | 10.26 ms | 11.62 ms | 45.71 ms | 0.00% | 100.00% | PASS (`exit=0`) | [`05_timeline_peak_50vu.txt`](05_timeline_peak_50vu.txt) / [JSON](05_timeline_peak_50vu.json) |
| **Timeline 100 VU** | 100 | 30s | 207,852 | **6,925.86** | 14.02 ms | 13.01 ms | 22.06 ms | 25.34 ms | 70.36 ms | 0.00% | 100.00% | PASS (`exit=0`) | [`06_timeline_peak_100vu.txt`](06_timeline_peak_100vu.txt) / [JSON](06_timeline_peak_100vu.json) |
| **Timeline 200 VU** | 200 | 30s | 233,360 | **7,773.23** | 25.27 ms | 23.70 ms | 39.55 ms | 44.81 ms | 189.26 ms | 0.00% | 100.00% | PASS (`exit=0`) | [`07_timeline_peak_200vu.txt`](07_timeline_peak_200vu.txt) / [JSON](07_timeline_peak_200vu.json) |
| **Hot 50 VU** | 50 | 30s | 189,188 | **6,304.43** | 7.57 ms | 7.19 ms | 10.69 ms | 12.07 ms | 54.19 ms | 0.00% | 100.00% | PASS (`exit=0`) | [`08_hot_peak_50vu.txt`](08_hot_peak_50vu.txt) / [JSON](08_hot_peak_50vu.json) |
| **Following 50 VU** | 50 | 30s | 44,513 | **1,479.99** | 33.36 ms | 31.38 ms | 48.85 ms | 55.31 ms | 125.07 ms | 0.00% | 100.00% | PASS (`exit=0`) | [`09_following_peak_50vu.txt`](09_following_peak_50vu.txt) / [JSON](09_following_peak_50vu.json) |
| **Recommend 50 VU** | 50 | 30s | 1,658 | 54.30 | 913.80 ms | 874.05 ms | 1,397.50 ms | **1,565.61 ms** | 2,031.95 ms | 0.00% | 100.00% | FAIL (`p95>500ms`, `exit=99`) | [`10_recommend_peak_50vu.txt`](10_recommend_peak_50vu.txt) / [JSON](10_recommend_peak_50vu.json) |

---

## 四、Prometheus 缓存命中率与服务端指标增量统计

对比 [`metrics_before.txt`](metrics_before.txt) 与 [`metrics_after.txt`](metrics_after.txt)：

### 1. 多级缓存命中率（全轮累计）
| 缓存层级 (`area`) | 命中次数 (`hit`) | 未命中次数 (`miss`) | 命中率 | 说明 |
| :--- | :---: | :---: | :---: | :--- |
| **Page 页缓存** (`page`) | 643,666 | 1,147 | **99.82%** | Timeline 首页 5s TTL + `singleflight` 合并回源 |
| **Card 卡片缓存** (`card`) | 8,818,550 | 0 | **100.00%** | 15min TTL，四类流共享 `MGET` 批量命中 |
| **Stat 计数缓存** (`stat`) | 8,818,240 | 310 | **99.996%** | 15s TTL，四类流共享 `Pipeline HGetAll` 批量命中 |

### 2. 服务端按场景请求计数增量 (`gcfeed_feed_requests_total`)
| 场景 (`scene`) | 成功数 (`success`) | 失败数 (`error`) |
| :--- | :---: | :---: |
| `timeline` | 644,783 | 0 |
| `hot` | 189,788 | 0 |
| `following` | 45,112 | 0 |
| `recommend` | 2,172 | 0 |

---

## 五、核心结论与 100 → 50,000 视频扩容对比洞察

1. **多级缓存成功隔离了数据量暴涨对核心读链路的影响（500 倍数据量下稳健运行）**
   - 视频表从 100 条扩容到 50,000 条后，`Timeline` 在 200 VU 极限压测下依然维持 **`7,773.23 QPS`**、P95 **`44.81 ms`**、**`0.00%` 错误率**，页缓存命中率达 **`99.82%`**（64.4 万次请求仅 1,147 次回源）。
   - `Hot` 在满页返回 10 条、50 VU 极限压测下达到 **`6,304.43 QPS`**、P95 **`12.07 ms`**、**`0.00%` 错误率**。
   - `Following` 在 50 VU 极限压测下达到 **`1,479.99 QPS`**、P95 **`55.31 ms`**、**`0.00%` 错误率**（相比 100 视频时因高频短连接重建导致连接池端口耗尽的问题，5 万数据下查询单次耗时拉长后未再触发 `TIME_WAIT` 耗尽，但仍建议将 `database.go` 中 `SetMaxIdleConns(10)` 调至与 `SetMaxOpenConns(50)` 对齐，并将 `BatchGetViewerActionStates` 改为优先查 Redis）。
2. **50,000 条规模精准打出了 Recommend 候选池的真实 SQL 瓶颈**
   - 在 100 条数据时，`Recommend` 20 VU P95 仅 `42.20 ms`、50 VU 可达 `1,090 QPS`，完全掩盖了底层排序代价；
   - 扩容到 50,000 条后，20 VU P95 升至 `309.80 ms`，50 VU 极限压测下 **QPS 降至 `54.30`、P95 飙升至 `1,565.61 ms`**（唯一未通过阈值的场景，但 0 报错）。
   - **根因实锤**（`gcfeed-api` 慢查询日志）：[`recommendation/gorm.go#L41-L62`](../apps/api/internal/infra/persistence/recommendation/gorm.go#L41-L62) 的 `ListCandidatePool` 每次请求都在执行动态表达式排序：
     `ORDER BY (COALESCE(vs.like_count, 0)*3 + ...) DESC, v.published_at DESC, v.id DESC LIMIT 80`，
     无法走索引，导致每次请求在 MySQL 内对 5 万条记录做 `JOIN + Filesort`（单条 SQL 耗时 `260~310 ms`）。
   - **对应演进方案**：将推荐热门候选池（Top 500）预计算/定时同步到 Redis ZSET（或复用 Hot 榜单/物化热度列 + 索引），在线查询直接从 Redis 拉取候选集再在内存过滤曝光和算向量相似度，即可把 Recommend P95 从秒级压回几十毫秒。
3. **原始产出目录**：[`exit_codes.json`](exit_codes.json)、[`metrics_before.txt`](metrics_before.txt)、[`metrics_after.txt`](metrics_after.txt) 及全部 10 组 `.txt` / `.json`。
