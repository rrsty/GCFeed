package infradatabase

import (
	"database/sql"
	"fmt"
	"time"

	infraconfig "GCFeed/internal/infra/config"

	_ "github.com/go-sql-driver/mysql"
)

// New 创建 MySQL 连接池，并用 Ping 提前验证配置是否可用。
func New(dbcfg infraconfig.DatabaseConfig) (*sql.DB, error) {
	// parseTime=true 让 DATETIME/TIMESTAMP 自动映射为 time.Time。
	dsn := fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=true&loc=Local",
		dbcfg.User,
		dbcfg.Password,
		dbcfg.Host,
		dbcfg.Port,
		dbcfg.Name,
	)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}

	// 配置连接池容量与生命周期，防止被 MySQL wait_timeout 断开后产生僵尸连接。
	db.SetMaxIdleConns(10)
	db.SetMaxOpenConns(50)
	db.SetConnMaxLifetime(30 * time.Minute)
	db.SetConnMaxIdleTime(5 * time.Minute)

	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}

	return db, nil
}
