package redis

import (
	"context"
	"errors"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// Dedup records keys with a TTL for idempotent processing. It is best-effort,
// not a source of truth: TTL'd keys can be lost on a Redis restart, which is
// acceptable because the outbox (see internal/infra/outbox) already makes
// event delivery at-least-once, so downstream consumers must tolerate rare
// duplicates regardless.
type Dedup struct {
	client *goredis.Client
	ttl    time.Duration
}

// NewDedup creates a Dedup backed by the given client.
func NewDedup(client *goredis.Client, ttl time.Duration) *Dedup {
	return &Dedup{client: client, ttl: ttl}
}

// MarkProcessed atomically marks key as processed, returning true if it was
// newly set and false if it already existed.
func (d *Dedup) MarkProcessed(ctx context.Context, key string) (bool, error) {
	err := d.client.SetArgs(ctx, key, 1, goredis.SetArgs{Mode: "NX", TTL: d.ttl}).Err()
	if errors.Is(err, goredis.Nil) {
		return false, nil // key already existed, NX condition not met
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// Unmark removes a previously marked key.
func (d *Dedup) Unmark(ctx context.Context, key string) error {
	return d.client.Del(ctx, key).Err()
}
