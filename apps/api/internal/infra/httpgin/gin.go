package infrahttpgin

import (
	infraconfig "GCFeed/internal/infra/config"
	inframetrics "GCFeed/internal/infra/metrics"
	"context"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
)

// Init 创建 Gin 引擎，并使用默认日志和恢复中间件。
func Init() *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	g := gin.Default()
	g.Use(inframetrics.HTTPMiddleware())
	return g
}

// Run 根据配置端口启动 HTTP 服务。
func Run(cfg *infraconfig.Config, g *gin.Engine) error {
	return RunWithContext(context.Background(), cfg, g)
}

// RunWithContext 启动带基础超时和优雅停机的 HTTP 服务。
func RunWithContext(ctx context.Context, cfg *infraconfig.Config, g *gin.Engine) error {
	server := &http.Server{
		Addr:              ":" + strconv.Itoa(cfg.Port),
		Handler:           g,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			return err
		}
		return <-errCh
	}
}
