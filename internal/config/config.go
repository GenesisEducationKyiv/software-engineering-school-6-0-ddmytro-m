// Package config provides configuration management for the application.
package config

import (
	"errors"
	"fmt"
	"os"

	"github.com/joho/godotenv"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/utils"
)

// Config contains fields common to all services.
type Config struct {
	AppEnv   string
	LogLevel string
}

// LoadEnv loads .env.<APP_ENV> then .env. Returns an error only if a file
// exists but cannot be parsed; missing files are silently skipped.
func LoadEnv() error {
	env := utils.GetEnv("APP_ENV", "development")
	if err := godotenv.Load(fmt.Sprintf(".env.%s", env)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("loading .env.%s: %w", env, err)
	}
	if err := godotenv.Load(); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("loading .env: %w", err)
	}
	return nil
}

func loadBaseConfig() Config {
	return Config{
		AppEnv:   utils.GetEnv("APP_ENV", "development"),
		LogLevel: utils.GetEnv("LOG_LEVEL", "info"),
	}
}