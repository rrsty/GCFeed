package main

import (
	"context"
	"database/sql"
	"log"
	"os/signal"
	"syscall"
	"time"

	applicationembedding "GCFeed/internal/application/embedding"
	applicationexposure "GCFeed/internal/application/exposure"
	applicationinteraction "GCFeed/internal/application/interaction"
	applicationvideo "GCFeed/internal/application/video"
	infracache "GCFeed/internal/infra/cache"
	infraconfig "GCFeed/internal/infra/config"
	infradatabase "GCFeed/internal/infra/database"
	inframetrics "GCFeed/internal/infra/metrics"
	inframq "GCFeed/internal/infra/mq"
	infraembedding "GCFeed/internal/infra/persistence/embedding"
	infrafeed "GCFeed/internal/infra/persistence/feed"
	infrainteraction "GCFeed/internal/infra/persistence/interaction"
	migration "GCFeed/internal/infra/persistence/migration"

	gormmysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
)

const configPath = "./configs/config.yaml"

func main() {
	cfg, err := infraconfig.LoadConfig(configPath)
	if err != nil {
		log.Fatalf("load config failed: %v", err)
	}
	if cfg.RabbitMQ.URL == "" {
		log.Fatal("rabbitmq url is required for worker")
	}
	if cfg.Redis.Addr == "" {
		log.Fatal("redis addr is required for worker")
	}

	sqlDB, err := infradatabase.New(cfg.Database)
	if err != nil {
		log.Fatalf("init database failed: %v", err)
	}
	defer closeSQL(sqlDB)

	gormDB, err := gorm.Open(gormmysql.New(gormmysql.Config{Conn: sqlDB}), &gorm.Config{})
	if err != nil {
		log.Fatalf("init gorm failed: %v", err)
	}
	if err := migration.AutoMigrate(gormDB); err != nil {
		log.Fatalf("auto migrate failed: %v", err)
	}

	rabbitMQ, err := inframq.NewRabbitMQ(cfg.RabbitMQ)
	if err != nil {
		log.Fatalf("init rabbitmq failed: %v", err)
	}
	defer closeRabbitMQ(rabbitMQ)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		if err := inframetrics.RunServer(ctx, ":9091"); err != nil {
			log.Printf("metrics server failed: %v", err)
		}
	}()

	cleanupWorkers, err := startWorkers(ctx, cfg, gormDB, rabbitMQ)
	if err != nil {
		log.Fatalf("start workers failed: %v", err)
	}
	if cleanupWorkers != nil {
		defer cleanupWorkers()
	}
	log.Println("gcfeed worker is running")
	<-ctx.Done()
	log.Println("gcfeed worker stopped")
}

func startWorkers(ctx context.Context, cfg *infraconfig.Config, gormDB *gorm.DB, rabbitMQ *inframq.RabbitMQ) (func(), error) {
	redisClient := infracache.NewRedisClient(cfg.Redis)
	feedCache := infracache.NewFeedCache(redisClient)

	interactionRepo := infrainteraction.New(gormDB)
	// 路线 A：启用窗口攒批刷库（每 200ms 或每 50 条合并一次 video_stat 原子更新），大幅降低爆款视频 DB 写压力。
	actionWorker := applicationinteraction.NewActionWorker(
		interactionRepo,
		rabbitMQ,
		applicationinteraction.WithBatchStatFlush(200*time.Millisecond, 50),
	)
	if err := actionWorker.Start(ctx); err != nil {
		_ = redisClient.Close()
		return nil, err
	}

	feedRepo := infrafeed.New(gormDB)
	feedPreheater := applicationvideo.NewFeedPreheater(feedRepo, feedCache)
	fanoutWorker := applicationvideo.NewFanoutWorker(feedRepo, rabbitMQ, feedCache, feedPreheater)
	if err := fanoutWorker.Start(ctx); err != nil {
		_ = redisClient.Close()
		return nil, err
	}

	embeddingRepo := infraembedding.New(gormDB)
	embeddingService := applicationembedding.New(embeddingRepo, nil)
	embeddingWorker := applicationembedding.NewVideoEmbeddingWorker(embeddingService, rabbitMQ)
	if err := embeddingWorker.Start(ctx); err != nil {
		_ = redisClient.Close()
		return nil, err
	}

	// 消费观看行为事件，防止 gcfeed.exposure.view_event_recorded 队列无消费者积压
	if err := rabbitMQ.ConsumeViewEventRecorded(ctx, func(c context.Context, event *applicationexposure.ViewEventRecordedEvent) error {
		if event != nil {
			inframetrics.ObserveWorkerJob("view_event_recorded_worker", 0, nil)
		}
		return nil
	}); err != nil {
		_ = redisClient.Close()
		return nil, err
	}

	cleanup := func() {
		flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := actionWorker.Close(flushCtx); err != nil {
			log.Printf("action worker close/flush failed: %v", err)
		}
		_ = redisClient.Close()
	}
	return cleanup, nil
}

func closeSQL(db *sql.DB) {
	if db != nil {
		_ = db.Close()
	}
}

func closeRabbitMQ(rabbitMQ *inframq.RabbitMQ) {
	if rabbitMQ != nil {
		_ = rabbitMQ.Close()
	}
}
