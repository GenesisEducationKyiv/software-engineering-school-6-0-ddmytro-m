package redis

import (
	"sync"

	"github.com/ddmytro-m/github-scanner/internal/config"
	"github.com/redis/go-redis/v9"
)

var (
	instance *redis.Client
	once     sync.Once
)

func Get() *redis.Client {
	once.Do(func() {
		instance = redis.NewClient(&redis.Options{
			Addr: config.Get().Redis.Addr,
		})
	})

	return instance
}
