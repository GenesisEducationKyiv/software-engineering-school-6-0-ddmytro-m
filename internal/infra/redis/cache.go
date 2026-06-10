package redis

import (
	"context"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// Cache provides a Redis-backed implementation of the GitHub client's caching mechanism.
type Cache struct {
	client *goredis.Client
}

// NewCacheWithAddr creates a new instance of the Cache with a default Redis client.
func NewCacheWithAddr(addr string) *Cache {
	return &Cache{client: GetClient(addr)}
}

// NewCacheWithClient creates a new instance of the Cache with a provided Redis client.
func NewCacheWithClient(client *goredis.Client) *Cache {
	return &Cache{client: client}
}

// Get retrieves the value associated with the given key from the Redis cache.
func (c *Cache) Get(ctx context.Context, key string) ([]byte, error) {
	return c.client.Get(ctx, key).Bytes()
}

// Set stores the value in the Redis cache with the specified key and time-to-live (TTL).
func (c *Cache) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	return c.client.Set(ctx, key, value, ttl).Err()
}
