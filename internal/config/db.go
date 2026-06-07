package config

import (
	"fmt"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/utils"
)

// LoadDBDSN reads database connection env vars and returns a DSN string.
func LoadDBDSN() string {
	return fmt.Sprintf(
		"host=%s user=%s password=%s dbname=%s port=%s sslmode=%s TimeZone=UTC",
		utils.MustGetEnv("DB_HOST"),
		utils.MustGetEnv("DB_USER"),
		utils.MustGetEnv("DB_PASSWORD"),
		utils.MustGetEnv("DB_NAME"),
		utils.GetEnv("DB_PORT", "5432"),
		utils.GetEnv("DB_SSLMODE", "disable"),
	)
}
