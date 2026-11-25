package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	infraconfig "GCFeed/internal/infra/config"
	infradatabase "GCFeed/internal/infra/database"
	infrahttpgin "GCFeed/internal/infra/httpgin"
	interfaceshttprouter "GCFeed/internal/interfaces/http/router"
)

const configPath = "./configs/config.yaml"

func main() {
	// 启动顺序保持简单：配置 -> 数据库 -> Gin -> 路由 -> 启动服务。
	cfg, err := infraconfig.LoadConfig(configPath)
	if err != nil {
		log.Fatalf("load config failed: %v", err)
	}
	log.Printf(
		"config loaded: port=%d database=%s:%d/%s jwt_access_ttl=%s",
		cfg.Port,
		cfg.Database.Host,
		cfg.Database.Port,
		cfg.Database.Name,
		cfg.JWT.AccessTTL,
	)

	// 数据库连接使用 database/sql 连接池，后续会被 GORM 复用。
	db, err := infradatabase.New(cfg.Database)
	if err != nil {
		log.Fatalf("init database failed: %v", err)
	}
	defer func() {
		_ = db.Close()
	}()
	log.Println("database connection initialized")

	// Gin 引擎只负责 HTTP 入口，业务依赖在 router.RegisterWithCleanup 中装配。
	g := infrahttpgin.Init()
	log.Println("gin engine initialized")

	// router.RegisterWithCleanup 完成仓储、Service、Handler 和中间件组装，并返回资源释放回调。
	cleanup, err := interfaceshttprouter.RegisterWithCleanup(g, cfg, db)
	if err != nil {
		log.Fatalf("init router failed: %v", err)
	}
	if cleanup != nil {
		defer cleanup()
	}
	log.Println("router registered")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// RunWithContext 在收到 SIGINT/SIGTERM 时停止接收新请求并等待在途请求完成后优雅退出。
	log.Println("server is running")
	if err := infrahttpgin.RunWithContext(ctx, cfg, g); err != nil {
		log.Fatalf("run server failed: %v", err)
	}
	log.Println("server stopped gracefully")
}
