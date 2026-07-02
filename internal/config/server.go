package config

// ServerConfig holds all configuration for the API server and scanner service.
type ServerConfig struct {
	Config
	DBDSN    string
	GitHub   GithubConfig
	Scanner  ScannerConfig
	Redis    RedisConfig
	RabbitMQ RabbitMQConfig
	Outbox   OutboxConfig
}

// LoadServerConfig reads all env vars required by the server service.
func LoadServerConfig() ServerConfig {
	return ServerConfig{
		Config:   loadBaseConfig(),
		DBDSN:    LoadDBDSN(),
		GitHub:   LoadGitHubConfig(),
		Scanner:  LoadScannerConfig(),
		Redis:    LoadRedisConfig(),
		RabbitMQ: LoadRabbitMQConfig(),
		Outbox:   LoadOutboxConfig(),
	}
}
