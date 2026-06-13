package config

import "github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/utils"

// RabbitMQConfig holds RabbitMQ connection and shared retry-topology settings.
// The retry policy is shared so every service declares identical tier queues.
type RabbitMQConfig struct {
	URL                string
	RetryTTLSeconds    int
	RetryBackoffFactor int
	MaxRetryAttempts   int
}

// LoadRabbitMQConfig reads RabbitMQ connection and retry env vars.
func LoadRabbitMQConfig() RabbitMQConfig {
	return RabbitMQConfig{
		URL:                utils.GetEnv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"),
		RetryTTLSeconds:    utils.GetEnvAs("RABBITMQ_RETRY_TTL_SECONDS", 30),
		RetryBackoffFactor: utils.GetEnvAs("RABBITMQ_RETRY_BACKOFF_FACTOR", 2),
		MaxRetryAttempts:   utils.GetEnvAs("RABBITMQ_MAX_RETRY_ATTEMPTS", 5),
	}
}
