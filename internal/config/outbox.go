package config

import (
	"time"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/utils"
)

// OutboxConfig holds the configuration for the outbox relay.
type OutboxConfig struct {
	PollInterval time.Duration
	BatchSize    int
}

// LoadOutboxConfig reads outbox relay env vars.
func LoadOutboxConfig() OutboxConfig {
	return OutboxConfig{
		PollInterval: time.Duration(utils.GetEnvAs("OUTBOX_POLL_INTERVAL_SECONDS", 2)) * time.Second,
		BatchSize:    utils.GetEnvAs("OUTBOX_BATCH_SIZE", 50),
	}
}
