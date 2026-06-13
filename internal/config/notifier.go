package config

import "github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/utils"

// NotifierConfig holds all configuration for the notifier service.
type NotifierConfig struct {
	Config
	RabbitMQ           RabbitMQConfig
	Redis              RedisConfig
	Workers            int
	PrefetchCount      int
	MaxRetryAttempts   int
	RetryTTLSeconds    int
	RetryBackoffFactor int
	DedupTTLHours      int
}

// LoadNotifierConfig reads all env vars required by the notifier service.
func LoadNotifierConfig() NotifierConfig {
	return NotifierConfig{
		Config:             loadBaseConfig(),
		RabbitMQ:           LoadRabbitMQConfig(),
		Redis:              LoadRedisConfig(),
		Workers:            utils.GetEnvAs("NOTIFIER_WORKERS", 3),
		PrefetchCount:      utils.GetEnvAs("NOTIFIER_PREFETCH_COUNT", 10),
		MaxRetryAttempts:   utils.GetEnvAs("NOTIFIER_MAX_RETRY_ATTEMPTS", 5),
		RetryTTLSeconds:    utils.GetEnvAs("NOTIFIER_RETRY_TTL_SECONDS", 30),
		RetryBackoffFactor: utils.GetEnvAs("NOTIFIER_RETRY_BACKOFF_FACTOR", 2),
		DedupTTLHours:      utils.GetEnvAs("NOTIFIER_DEDUP_TTL_HOURS", 24),
	}
}
