package config

import "github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/utils"

// MailerConfig holds all configuration for the mailer service.
type MailerConfig struct {
	Config
	SMTP    SMTPConfig
	Redis   RedisConfig
	Workers int
}

// LoadMailerConfig reads all env vars required by the mailer service.
func LoadMailerConfig() MailerConfig {
	return MailerConfig{
		Config:  loadBaseConfig(),
		SMTP:    LoadSMTPConfig(),
		Redis:   LoadRedisConfig(),
		Workers: utils.GetEnvAs("MAILER_WORKERS", 3),
	}
}