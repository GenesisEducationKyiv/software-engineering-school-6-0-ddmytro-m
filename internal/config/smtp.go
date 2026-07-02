package config

import "github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/utils"

// SMTPConfig holds the configuration for the SMTP client.
type SMTPConfig struct {
	Host        string
	Port        int
	Username    string
	SenderEmail string
	Password    string
	From        string
}

// LoadSMTPConfig reads SMTP env vars.
func LoadSMTPConfig() SMTPConfig {
	username := utils.MustGetEnv("SMTP_USER")
	return SMTPConfig{
		Host:        utils.MustGetEnv("SMTP_HOST"),
		Port:        utils.GetEnvAs("SMTP_PORT", 587),
		Username:    username,
		SenderEmail: utils.GetEnv("SMTP_SENDER_EMAIL", username),
		Password:    utils.MustGetEnv("SMTP_PASS"),
		From:        utils.GetEnv("SMTP_FROM", username),
	}
}
