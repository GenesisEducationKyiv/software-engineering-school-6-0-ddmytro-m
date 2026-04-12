package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisStream struct {
	client     *redis.Client
	stream     string
	deadLetter string
}

func NewRedisStream(client *redis.Client, stream string) *RedisStream {
	if client == nil {
		return nil
	}

	return &RedisStream{
		client:     client,
		stream:     stream,
		deadLetter: fmt.Sprintf("%s:dead-letter", stream),
	}
}

func (n *RedisStream) EnsureConsumerGroup(ctx context.Context, group string) error {
	err := n.client.XGroupCreateMkStream(ctx, n.stream, group, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return err
	}
	return nil
}

func (n *RedisStream) Publish(ctx context.Context, msg any) error {
	payload, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	return n.client.XAdd(ctx, &redis.XAddArgs{
		Stream: n.stream,
		Values: map[string]any{"payload": payload},
	}).Err()
}

func (n *RedisStream) ReadGroup(ctx context.Context, group, consumer string, count int64, block time.Duration) ([]redis.XMessage, error) {
	res, err := n.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  []string{n.stream, ">"},
		Count:    count,
		Block:    block,
	}).Result()
	if err != nil {
		return nil, err
	}
	if len(res) == 0 {
		return nil, nil
	}
	return res[0].Messages, nil
}

func (n *RedisStream) Ack(ctx context.Context, group string, ids ...string) error {
	return n.client.XAck(ctx, n.stream, group, ids...).Err()
}

func (n *RedisStream) AutoClaim(ctx context.Context, group, consumer string, minIdle time.Duration, start string, count int64) (msgs []redis.XMessage, next string, err error) {
	msgs, next, err = n.client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   n.stream,
		Group:    group,
		Consumer: consumer,
		MinIdle:  minIdle,
		Start:    start,
		Count:    count,
	}).Result()

	if err != nil {
		return nil, "", err
	}

	return msgs, next, nil
}

func (n *RedisStream) PublishDeadLetter(ctx context.Context, msg any) error {
	payload, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("dead-letter marshal: %w", err)
	}

	return n.client.XAdd(ctx, &redis.XAddArgs{
		Stream: n.deadLetter,
		Values: map[string]any{"payload": payload},
	}).Err()
}
