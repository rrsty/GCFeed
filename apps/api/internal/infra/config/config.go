package infraconfig

import (
	"errors"
	"os"
	"strings"

	"github.com/goccy/go-yaml"
)

var ErrEmptyConfigPath = errors.New("config file path is empty")
var ErrReadConfigFailed = errors.New("read config file failed")
var ErrUnmarshalConfigFailed = errors.New("unmarshal config failed")
var ErrInsecureProductionSecret = errors.New("insecure default secret/token in production environment")

// LoadConfig 读取 YAML 配置文件，并反序列化为应用启动配置。
func LoadConfig(path string) (*Config, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, ErrEmptyConfigPath
	}

	content, err := os.ReadFile(path)
	if err != nil {
		return nil, ErrReadConfigFailed
	}
	cfg := &Config{}
	if err := yaml.Unmarshal(content, cfg); err != nil {
		return nil, ErrUnmarshalConfigFailed
	}

	applyEnvOverrides(cfg)

	// 生产环境下禁止使用默认弱口令/示例密钥，杜绝身份伪造漏洞
	appEnv := strings.ToLower(strings.TrimSpace(os.Getenv("APP_ENV")))
	if appEnv == "production" || appEnv == "prod" {
		if cfg.JWT.Secret == "replace-with-strong-secret" || len(cfg.JWT.Secret) < 16 {
			return nil, ErrInsecureProductionSecret
		}
		if cfg.Internal.Token == "replace-with-internal-token" || len(cfg.Internal.Token) < 16 {
			return nil, ErrInsecureProductionSecret
		}
	}

	return cfg, nil
}

func applyEnvOverrides(cfg *Config) {
	if secret := os.Getenv("JWT_SECRET"); secret != "" {
		cfg.JWT.Secret = secret
	}
	if token := os.Getenv("INTERNAL_TOKEN"); token != "" {
		cfg.Internal.Token = token
	}
	if dbPass := os.Getenv("MYSQL_PASSWORD"); dbPass != "" {
		cfg.Database.Password = dbPass
	} else if dbPass := os.Getenv("DB_PASSWORD"); dbPass != "" {
		cfg.Database.Password = dbPass
	}
	if redisPass := os.Getenv("REDIS_PASSWORD"); redisPass != "" {
		cfg.Redis.Password = redisPass
	}
}
