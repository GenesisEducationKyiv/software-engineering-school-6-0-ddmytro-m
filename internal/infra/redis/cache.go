package redis

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// Cache provides a Redis-backed implementation of the GitHub client's caching mechanism.
type Cache struct {
	client *redis.Client
}

func NewCache() *Cache {
	return &Cache{client: Get()}
}

func NewCacheWithClient(client *redis.Client) *Cache {
	return &Cache{client: client}
}

func (c *Cache) Get(ctx context.Context, key string) ([]byte, error) {
	return c.client.Get(ctx, key).Bytes()
}
func (c *Cache) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	return c.client.Set(ctx, key, value, ttl).Err()
}
