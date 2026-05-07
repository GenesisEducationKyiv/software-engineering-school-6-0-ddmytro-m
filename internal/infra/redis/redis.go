// Package redis provides Redis client and streaming functionalities.
package redis

import (
	"sync"

	"github.com/redis/go-redis/v9"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/config"
)

var (
	instance *redis.Client
	once     sync.Once
)

// Get returns a singleton instance of the Redis client.
func Get() *redis.Client {
	once.Do(func() {
		instance = redis.NewClient(&redis.Options{
			Addr: config.Get().Redis.Addr,
		})
	})

	return instance
}
