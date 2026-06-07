// Package redis provides Redis client and streaming functionalities.
package redis

import (
	"sync"

	goredis "github.com/redis/go-redis/v9"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/config"
)

var (
	instance *goredis.Client
	once     sync.Once
)

// GetClient returns a singleton instance of the Redis client.
func GetClient() *goredis.Client {
	once.Do(func() {
		instance = goredis.NewClient(&goredis.Options{
			Addr: config.Get().Redis.Addr,
		})
	})

	return instance
}
