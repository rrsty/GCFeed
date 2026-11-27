package main

import (
	applicationaccount "GCFeed/internal/application/account"
	applicationinteraction "GCFeed/internal/application/interaction"
	infracache "GCFeed/internal/infra/cache"
	infraconfig "GCFeed/internal/infra/config"
	infrahttpgin "GCFeed/internal/infra/httpgin"
	infrajwt "GCFeed/internal/infra/jwt"
	inframq "GCFeed/internal/infra/mq"
	infraaccount "GCFeed/internal/infra/persistence/account"
	infrainteraction "GCFeed/internal/infra/persistence/interaction"
	interfaceshttpaccount "GCFeed/internal/interfaces/http/account"
	interfaceshttpinteraction "GCFeed/internal/interfaces/http/interaction"
	interfaceshttpmiddleware "GCFeed/internal/interfaces/http/middleware"
	"context"
	"fmt"
	"log"
	"net/http"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	benchExchange   = "gcfeed.bench.interaction"
	benchQueue      = "gcfeed.bench.action_changed"
	benchRoutingKey = "bench.action_changed"
)

func runBenchServer(env *BenchEnv, mode string, port int, jwtSecret string) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	jwtManager, err := infrajwt.NewManager(jwtSecret, "1h")
	if err != nil {
		return err
	}
	accountRepo := infraaccount.New(env.GormDB)
	accountService := applicationaccount.New(accountRepo, jwtManager)
	accountHandler := interfaceshttpaccount.New(accountService)

	interactionRepo := infrainteraction.New(env.GormDB)
	countingRepo := &countingActionRepo{Repository: interactionRepo}

	var interactionOpts []applicationinteraction.Option
	var rabbitMQ *inframq.RabbitMQ
	var actionWorker *applicationinteraction.ActionWorker

	switch mode {
	case "sync":
		// No async action pipeline -> calls setActionSync directly against MySQL
	case "legacy-watch":
		legacyStore := NewLegacyWatchFeedCache(env.Redis)
		rabbitMQ, err = newBenchRabbitMQ(env.AMQPURL)
		if err != nil {
			return err
		}
		defer rabbitMQ.Close()
		interactionOpts = append(interactionOpts, applicationinteraction.WithAsyncActionPipeline(legacyStore, rabbitMQ))
		actionWorker = applicationinteraction.NewActionWorker(countingRepo, rabbitMQ)
		if err := actionWorker.Start(ctx); err != nil {
			return err
		}
	case "lua-batch":
		feedCache := infracache.NewFeedCache(env.Redis)
		rabbitMQ, err = newBenchRabbitMQ(env.AMQPURL)
		if err != nil {
			return err
		}
		defer rabbitMQ.Close()
		interactionOpts = append(interactionOpts, applicationinteraction.WithAsyncActionPipeline(feedCache, rabbitMQ))
		actionWorker = applicationinteraction.NewActionWorker(
			countingRepo,
			rabbitMQ,
			applicationinteraction.WithBatchStatFlush(200*time.Millisecond, 50),
		)
		if err := actionWorker.Start(ctx); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown serve-mode: %s", mode)
	}

	interactionService := applicationinteraction.New(interactionRepo, interactionOpts...)
	interactionHandler := interfaceshttpinteraction.New(interactionService)

	gin.SetMode(gin.ReleaseMode)
	g := gin.New()
	g.Use(gin.Recovery())

	g.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok", "mode": mode})
	})

	g.GET("/bench/stats", func(c *gin.Context) {
		videoID, _ := strconv.ParseInt(c.DefaultQuery("video_id", strconv.FormatInt(benchVideoID, 10)), 10, 64)
		var dbLike int
		var dbActive int
		_ = env.SQLDB.QueryRowContext(c.Request.Context(), "SELECT like_count FROM video_stat WHERE video_id = ?", videoID).Scan(&dbLike)
		_ = env.SQLDB.QueryRowContext(c.Request.Context(), "SELECT COUNT(*) FROM interaction_action WHERE video_id = ? AND action_type = 'LIKE' AND status = 1", videoID).Scan(&dbActive)

		redisLikeV2, _ := env.Redis.HGet(c.Request.Context(), fmt.Sprintf("video:stat:v2:%d", videoID), "like_count").Result()
		redisJSONV1, _ := env.Redis.Get(c.Request.Context(), fmt.Sprintf("video:stat:v1:%d", videoID)).Result()

		qDepth := -1
		if mode != "sync" {
			qDepth = inspectQueueDepth(env.AMQPURL, benchQueue)
		}
		c.JSON(http.StatusOK, gin.H{
			"mode":               mode,
			"video_id":           videoID,
			"queue_depth":        qDepth,
			"worker_record_calls": countingRepo.recordCalls.Load(),
			"worker_flush_calls":  countingRepo.flushCalls.Load(),
			"db_like_count":      dbLike,
			"db_active_rows":     dbActive,
			"redis_v2_like":      redisLikeV2,
			"redis_v1_json":      redisJSONV1,
		})
	})

	g.POST("/bench/flush", func(c *gin.Context) {
		if actionWorker != nil {
			_ = actionWorker.Flush(c.Request.Context())
		}
		c.JSON(http.StatusOK, gin.H{"flushed": true})
	})

	authMiddleware := interfaceshttpmiddleware.NewJWTAuth(jwtManager)
	api := g.Group("/api")
	api.POST("/sessions", accountHandler.Login)
	api.PUT("/videos/:videoId/like", authMiddleware, interactionHandler.Like)
	api.DELETE("/videos/:videoId/like", authMiddleware, interactionHandler.Unlike)

	cfg := &infraconfig.Config{Port: port}
	log.Printf("abbench server listening on :%d mode=%s redis_db=%d", port, mode, env.RedisCfg.DB)
	err = infrahttpgin.RunWithContext(ctx, cfg, g)
	if actionWorker != nil {
		flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = actionWorker.Close(flushCtx)
		cancel()
	}
	return err
}

func newBenchRabbitMQ(amqpURL string) (*inframq.RabbitMQ, error) {
	return inframq.NewRabbitMQ(infraconfig.RabbitMQConfig{
		URL:                  amqpURL,
		InteractionExchange:  benchExchange,
		ActionChangedQueue:   benchQueue,
		ActionChangedRouting: benchRoutingKey,
	})
}

func inspectQueueDepth(amqpURL, queueName string) int {
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		return -1
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		return -1
	}
	defer ch.Close()
	q, err := ch.QueueDeclarePassive(queueName, true, false, false, false, nil)
	if err != nil {
		return -1
	}
	return q.Messages
}
