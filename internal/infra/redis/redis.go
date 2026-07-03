// Package redis provides a Redis client, response cache, and dedup store.
package redis

import (
	"sync"

	goredis "github.com/redis/go-redis/v9"
)

var (
	instance *goredis.Client
	once     sync.Once
)

// GetClient returns a singleton instance of the Redis client.
func GetClient(addr string) *goredis.Client {
	once.Do(func() {
		instance = goredis.NewClient(&goredis.Options{
			Addr: addr,
		})
	})

	return instance
}
