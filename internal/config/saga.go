package config

import (
	"time"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/utils"
)

// SagaReaperConfig holds the configuration for the onboarding-saga reaper.
type SagaReaperConfig struct {
	PollInterval time.Duration
	StaleAfter   time.Duration
}

// LoadSagaReaperConfig reads saga reaper env vars.
func LoadSagaReaperConfig() SagaReaperConfig {
	return SagaReaperConfig{
		PollInterval: time.Duration(utils.GetEnvAs("SAGA_REAPER_POLL_INTERVAL_SECONDS", 300)) * time.Second,
		StaleAfter:   time.Duration(utils.GetEnvAs("SAGA_STALE_AFTER_SECONDS", 1800)) * time.Second,
	}
}
