// Package redis provides Redis client and streaming functionalities.
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
