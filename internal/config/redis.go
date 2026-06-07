package config

import "github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/utils"

// RedisConfig holds the configuration for connecting to Redis.
type RedisConfig struct {
	Addr string
}

// LoadRedisConfig reads Redis connection env vars.
func LoadRedisConfig() RedisConfig {
	return RedisConfig{
		Addr: utils.GetEnv("REDIS_ADDR", "localhost:6379"),
	}
}