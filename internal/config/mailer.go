package config

import "github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/utils"

// MailerConfig holds all configuration for the mailer service.
type MailerConfig struct {
	Config
	SMTP          SMTPConfig
	RabbitMQ      RabbitMQConfig
	Workers       int
	PrefetchCount int
}

// LoadMailerConfig reads all env vars required by the mailer service.
func LoadMailerConfig() MailerConfig {
	return MailerConfig{
		Config:        loadBaseConfig(),
		SMTP:          LoadSMTPConfig(),
		RabbitMQ:      LoadRabbitMQConfig(),
		Workers:       utils.GetEnvAs("MAILER_WORKERS", 3),
		PrefetchCount: utils.GetEnvAs("MAILER_PREFETCH_COUNT", 10),
	}
}
