package config

import (
	"time"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/utils"
)

// ScannerConfig holds the configuration for the repository scanner worker.
type ScannerConfig struct {
	Workers          int
	QueueSize        int
	SafetyBuffer     float64
	ProducerInterval time.Duration
	MinInterval      time.Duration
}

// LoadScannerConfig reads scanner worker env vars.
func LoadScannerConfig() ScannerConfig {
	return ScannerConfig{
		Workers:          utils.GetEnvAs("SCANNER_WORKERS", 1),
		QueueSize:        utils.GetEnvAs("SCANNER_QUEUE_SIZE", 100),
		SafetyBuffer:     utils.GetEnvAs("SCANNER_SAFETY_BUFFER", 0.1),
		ProducerInterval: time.Duration(utils.GetEnvAs("SCANNER_PRODUCER_INTERVAL_SECONDS", 60)) * time.Second,
		MinInterval:      time.Duration(utils.GetEnvAs("SCANNER_MIN_INTERVAL_SECONDS", 900)) * time.Second,
	}
}
