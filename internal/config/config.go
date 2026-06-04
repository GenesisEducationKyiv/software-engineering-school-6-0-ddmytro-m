// Package config provides configuration management for the application.
package config

import (
	"fmt"
	"sync"
	"time"

	"github.com/joho/godotenv"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/utils"
)

// GithubConfig holds the configuration for the GitHub API client.
type GithubConfig struct {
	Token         string
	Timeout       time.Duration
	CacheTTL      time.Duration
	CacheErrorTTL time.Duration
}

// ScannerConfig holds the configuration for the repository scanner worker.
type ScannerConfig struct {
	Workers          int
	QueueSize        int
	SafetyBuffer     float64
	ProducerInterval time.Duration
	MinInterval      time.Duration
}

// SMTPConfig holds the configuration for the SMTP client used for sending emails.
type SMTPConfig struct {
	Host string
	Port int

	Username    string
	SenderEmail string
	Password    string

	From string
}

// RedisConfig holds the configuration for connecting to Redis.
type RedisConfig struct {
	Addr string
}

// Config holds the overall configuration for the application.
type Config struct {
	AppEnv   string
	LogLevel string
	DBDSN    string
	GitHub   GithubConfig
	Scanner  ScannerConfig
	SMTP     SMTPConfig
	Redis    RedisConfig
}

func getDSN() string {
	return fmt.Sprintf(
		"host=%s user=%s password=%s dbname=%s port=%s sslmode=%s TimeZone=UTC",
		utils.MustGetEnv("DB_HOST"),
		utils.MustGetEnv("DB_USER"),
		utils.MustGetEnv("DB_PASSWORD"),
		utils.MustGetEnv("DB_NAME"),
		utils.GetEnv("DB_PORT", "5432"),
		utils.GetEnv("DB_SSLMODE", "disable"),
	)
}

func getGithubConfig(env string) GithubConfig {
	var githubToken string
	if env == "production" {
		githubToken = utils.MustGetEnv("GITHUB_TOKEN")
	} else {
		githubToken = utils.GetEnv("GITHUB_TOKEN", "")
	}
	githubTimeoutSeconds := utils.GetEnvAs("GITHUB_TIMEOUT_SECONDS", 15)
	githubCacheTTLSeconds := utils.GetEnvAs("GITHUB_CACHE_TTL_SECONDS", 600)
	githubCacheErrorTTLSeconds := utils.GetEnvAs("GITHUB_CACHE_ERROR_TTL_SECODS", 60)

	return GithubConfig{
		Token:         githubToken,
		Timeout:       time.Duration(githubTimeoutSeconds) * time.Second,
		CacheTTL:      time.Duration(githubCacheTTLSeconds) * time.Second,
		CacheErrorTTL: time.Duration(githubCacheErrorTTLSeconds) * time.Second,
	}
}

func getScannerConfig() ScannerConfig {
	workers := utils.GetEnvAs("SCANNER_WORKERS", 1)
	queueSize := utils.GetEnvAs("SCANNER_QUEUE_SIZE", 100)
	safetyBuffer := utils.GetEnvAs("SCANNER_SAFETY_BUFFER", 0.1)
	producerIntervalSeconds := utils.GetEnvAs("SCANNER_PRODUCER_INTERVAL_SECONDS", 60)
	minIntervalSeconds := utils.GetEnvAs("SCANNER_MIN_INTERVAL_SECONDS", 900)

	return ScannerConfig{
		Workers:          workers,
		QueueSize:        queueSize,
		SafetyBuffer:     safetyBuffer,
		ProducerInterval: time.Duration(producerIntervalSeconds) * time.Second,
		MinInterval:      time.Duration(minIntervalSeconds) * time.Second,
	}
}

func getSMTPConfig() SMTPConfig {
	username := utils.MustGetEnv("SMTP_USER")
	senderEmail := utils.GetEnv("SMTP_SENDER_EMAIL", username)

	from := utils.GetEnv("SMTP_FROM", username)

	return SMTPConfig{
		Host:        utils.MustGetEnv("SMTP_HOST"),
		Port:        utils.GetEnvAs("SMTP_PORT", 587),
		Username:    username,
		SenderEmail: senderEmail,
		Password:    utils.MustGetEnv("SMTP_PASS"),
		From:        from,
	}
}

func getRedisConfig() RedisConfig {
	return RedisConfig{
		Addr: utils.GetEnv("REDIS_ADDR", "localhost:6379"),
	}
}

var (
	instance *Config
	once     sync.Once
)

// Get retrieves the global application configuration, loading it if necessary.
func Get() *Config {
	once.Do(func() {
		env := utils.GetEnv("APP_ENV", "development")

		_ = godotenv.Load(fmt.Sprintf(".env.%s", env))
		_ = godotenv.Load()

		instance = &Config{
			AppEnv:   env,
			LogLevel: utils.GetEnv("LOG_LEVEL", "info"),
			DBDSN:    getDSN(),
			GitHub:   getGithubConfig(env),
			Scanner:  getScannerConfig(),
			SMTP:     getSMTPConfig(),
			Redis:    getRedisConfig(),
		}
	})

	return instance
}
