package config

import "github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/utils"

// RabbitMQConfig holds the configuration for connecting to RabbitMQ.
type RabbitMQConfig struct {
	URL string
}

// LoadRabbitMQConfig reads RabbitMQ connection env vars.
func LoadRabbitMQConfig() RabbitMQConfig {
	return RabbitMQConfig{
		URL: utils.GetEnv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"),
	}
}
