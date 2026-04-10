package config

import (
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/joho/godotenv"
)

type Config struct {
	DBDSN       string
	GitHubToken string
	AppEnv      string
	LogLevel    string
}

var (
	instance *Config
	once     sync.Once
)

func Get() *Config {
	once.Do(func() {
		env := getEnv("APP_ENV", "development")

		_ = godotenv.Load(fmt.Sprintf(".env.%s", env))
		_ = godotenv.Load()

		dsn := fmt.Sprintf(
			"host=%s user=%s password=%s dbname=%s port=%s sslmode=%s TimeZone=UTC",
			mustGetEnv("DB_HOST"),
			mustGetEnv("DB_USER"),
			mustGetEnv("DB_PASSWORD"),
			mustGetEnv("DB_NAME"),
			getEnv("DB_PORT", "5432"),
			getEnv("DB_SSLMODE", "disable"),
		)

		var githubToken string
		if env == "production" {
			githubToken = mustGetEnv("GITHUB_TOKEN")
		} else {
			githubToken = getEnv("GITHUB_TOKEN", "")
		}

		instance = &Config{
			DBDSN:       dsn,
			GitHubToken: githubToken,
			AppEnv:      env,
			LogLevel:    getEnv("LOG_LEVEL", "info"),
		}

		fmt.Printf("configuration loaded for environment: %s\n", env)
	})

	return instance
}

func mustGetEnv(key string) string {
	value, ok := os.LookupEnv(key)
	if !ok {
		log.Fatalf("FATAL: environment variable %s is required but not set", key)
	}
	return value
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}
