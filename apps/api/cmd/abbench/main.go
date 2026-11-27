package main

import (
	infracache "GCFeed/internal/infra/cache"
	infraconfig "GCFeed/internal/infra/config"
	infradatabase "GCFeed/internal/infra/database"
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	gormmysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var (
	suiteFlag     = flag.String("suite", "all", "benchmark suite: cursor | cache-sep | write-micro | all | serve")
	serveModeFlag = flag.String("serve-mode", "lua-batch", "http server mode for -suite=serve: sync | legacy-watch | lua-batch")
	portFlag      = flag.Int("port", 18088, "http port for -suite=serve")
	mysqlHostFlag = flag.String("mysql-host", "127.0.0.1", "mysql host")
	mysqlPortFlag = flag.Int("mysql-port", 3308, "mysql port")
	mysqlUserFlag = flag.String("mysql-user", "root", "mysql user")
	mysqlPassFlag = flag.String("mysql-pass", "123456", "mysql password")
	mysqlDBFlag   = flag.String("mysql-db", "GCFeed", "mysql database name")
	redisAddrFlag = flag.String("redis-addr", "127.0.0.1:6381", "redis addr")
	redisDBFlag   = flag.Int("redis-db", 1, "redis db index (isolated from default db 0)")
	amqpURLFlag   = flag.String("amqp-url", "amqp://guest:guest@127.0.0.1:5672/", "rabbitmq url")
	jwtSecretFlag = flag.String("jwt-secret", "replace-with-strong-secret", "jwt secret matching docker api")
	outDirFlag    = flag.String("out-dir", "../../result/ab_compare", "output directory for benchmark json")
)

type LatencyStats struct {
	Count   int64   `json:"count"`
	Errors  int64   `json:"errors"`
	Elapsed float64 `json:"elapsed_sec"`
	QPS     float64 `json:"qps"`
	AvgMS   float64 `json:"avg_ms"`
	MinMS   float64 `json:"min_ms"`
	P50MS   float64 `json:"p50_ms"`
	P90MS   float64 `json:"p90_ms"`
	P95MS   float64 `json:"p95_ms"`
	P99MS   float64 `json:"p99_ms"`
	MaxMS   float64 `json:"max_ms"`
}

type BenchEnv struct {
	DBCfg    infraconfig.DatabaseConfig
	RedisCfg infraconfig.RedisConfig
	AMQPURL  string
	SQLDB    *sql.DB
	GormDB   *gorm.DB
	Redis    *redis.Client
	OutDir   string
}

func main() {
	flag.Parse()

	dbCfg := infraconfig.DatabaseConfig{
		Host:     *mysqlHostFlag,
		Port:     *mysqlPortFlag,
		User:     *mysqlUserFlag,
		Password: *mysqlPassFlag,
		Name:     *mysqlDBFlag,
	}
	sqlDB, err := infradatabase.New(dbCfg)
	if err != nil {
		log.Fatalf("connect mysql failed: %v", err)
	}
	defer sqlDB.Close()

	gormDB, err := gorm.Open(gormmysql.New(gormmysql.Config{Conn: sqlDB}), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		log.Fatalf("init gorm failed: %v", err)
	}

	redisCfg := infraconfig.RedisConfig{
		Addr: *redisAddrFlag,
		DB:   *redisDBFlag,
	}
	rdb := infracache.NewRedisClient(redisCfg)
	defer rdb.Close()
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		log.Fatalf("connect redis failed: %v", err)
	}

	if err := os.MkdirAll(*outDirFlag, 0o755); err != nil {
		log.Fatalf("create out-dir failed: %v", err)
	}

	env := &BenchEnv{
		DBCfg:    dbCfg,
		RedisCfg: redisCfg,
		AMQPURL:  *amqpURLFlag,
		SQLDB:    sqlDB,
		GormDB:   gormDB,
		Redis:    rdb,
		OutDir:   *outDirFlag,
	}

	switch *suiteFlag {
	case "cursor":
		if err := runCursorSuite(env); err != nil {
			log.Fatalf("cursor suite failed: %v", err)
		}
	case "cache-sep":
		if err := runCacheSepSuite(env); err != nil {
			log.Fatalf("cache-sep suite failed: %v", err)
		}
	case "write-micro":
		if err := runWriteMicroSuite(env); err != nil {
			log.Fatalf("write-micro suite failed: %v", err)
		}
	case "all":
		if err := runCursorSuite(env); err != nil {
			log.Fatalf("cursor suite failed: %v", err)
		}
		if err := runCacheSepSuite(env); err != nil {
			log.Fatalf("cache-sep suite failed: %v", err)
		}
		if err := runWriteMicroSuite(env); err != nil {
			log.Fatalf("write-micro suite failed: %v", err)
		}
	case "serve":
		if err := runBenchServer(env, *serveModeFlag, *portFlag, *jwtSecretFlag); err != nil {
			log.Fatalf("serve failed: %v", err)
		}
	default:
		log.Fatalf("unknown suite: %s", *suiteFlag)
	}
}

func calcLatencyStats(durs []time.Duration, elapsed time.Duration, errCount int64) LatencyStats {
	n := len(durs)
	sec := elapsed.Seconds()
	if sec <= 0 {
		sec = 1e-6
	}
	stats := LatencyStats{
		Count:   int64(n),
		Errors:  errCount,
		Elapsed: round4(sec),
		QPS:     round2(float64(n) / sec),
	}
	if n == 0 {
		return stats
	}

	sorted := make([]float64, n)
	var sum float64
	for i, d := range durs {
		ms := float64(d.Microseconds()) / 1000.0
		sorted[i] = ms
		sum += ms
	}
	sort.Float64s(sorted)

	stats.AvgMS = round3(sum / float64(n))
	stats.MinMS = round3(sorted[0])
	stats.P50MS = round3(percentile(sorted, 0.50))
	stats.P90MS = round3(percentile(sorted, 0.90))
	stats.P95MS = round3(percentile(sorted, 0.95))
	stats.P99MS = round3(percentile(sorted, 0.99))
	stats.MaxMS = round3(sorted[n-1])
	return stats
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * p)
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func round2(v float64) float64 {
	return float64(int64(v*100+0.5)) / 100
}

func round3(v float64) float64 {
	return float64(int64(v*1000+0.5)) / 1000
}

func round4(v float64) float64 {
	return float64(int64(v*10000+0.5)) / 10000
}

var trackedMySQLStatusKeys = []string{
	"Com_select",
	"Com_insert",
	"Com_update",
	"Handler_read_next",
	"Handler_read_key",
	"Innodb_row_lock_waits",
	"Innodb_row_lock_time",
	"Innodb_rows_updated",
}

func snapshotMySQLStatus(db *sql.DB) map[string]int64 {
	out := make(map[string]int64, len(trackedMySQLStatusKeys))
	rows, err := db.Query("SHOW GLOBAL STATUS WHERE Variable_name IN ('Com_select','Com_insert','Com_update','Handler_read_next','Handler_read_key','Innodb_row_lock_waits','Innodb_row_lock_time','Innodb_rows_updated')")
	if err != nil {
		return out
	}
	defer rows.Close()
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err == nil {
			if n, convErr := strconv.ParseInt(v, 10, 64); convErr == nil {
				out[k] = n
			}
		}
	}
	return out
}

func diffMySQLStatus(before, after map[string]int64) map[string]int64 {
	diff := make(map[string]int64, len(after))
	for k, v := range after {
		diff[k] = v - before[k]
	}
	return diff
}

func snapshotRedisCommands(ctx context.Context, rdb *redis.Client) int64 {
	raw, err := rdb.Info(ctx, "stats").Result()
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(raw, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "total_commands_processed:") {
			val := strings.TrimPrefix(line, "total_commands_processed:")
			n, _ := strconv.ParseInt(strings.TrimSpace(val), 10, 64)
			return n
		}
	}
	return 0
}

func snapshotRedisMemory(ctx context.Context, rdb *redis.Client) int64 {
	raw, err := rdb.Info(ctx, "memory").Result()
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(raw, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "used_memory:") {
			val := strings.TrimPrefix(line, "used_memory:")
			n, _ := strconv.ParseInt(strings.TrimSpace(val), 10, 64)
			return n
		}
	}
	return 0
}

func writeJSON(outDir, filename string, payload any) error {
	path := filepath.Join(outDir, filename)
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return err
	}
	fmt.Printf("==> Saved %s\n", path)
	return nil
}
