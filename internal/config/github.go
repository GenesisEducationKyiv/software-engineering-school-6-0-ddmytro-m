package config

import (
	"time"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/utils"
)

// GithubConfig holds the configuration for the GitHub API client.
type GithubConfig struct {
	Token         string
	Timeout       time.Duration
	CacheTTL      time.Duration
	CacheErrorTTL time.Duration
}

// LoadGitHubConfig reads GitHub API env vars.
func LoadGitHubConfig() GithubConfig {
	env := utils.GetEnv("APP_ENV", "development")
	var token string
	if env == "production" {
		token = utils.MustGetEnv("GITHUB_TOKEN")
	} else {
		token = utils.GetEnv("GITHUB_TOKEN", "")
	}
	return GithubConfig{
		Token:         token,
		Timeout:       time.Duration(utils.GetEnvAs("GITHUB_TIMEOUT_SECONDS", 15)) * time.Second,
		CacheTTL:      time.Duration(utils.GetEnvAs("GITHUB_CACHE_TTL_SECONDS", 600)) * time.Second,
		CacheErrorTTL: time.Duration(utils.GetEnvAs("GITHUB_CACHE_ERROR_TTL_SECODS", 60)) * time.Second,
	}
}