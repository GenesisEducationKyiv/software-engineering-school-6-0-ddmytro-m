package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Stream represents a Redis Stream client wrapper.
type Stream struct {
	client     *redis.Client
	stream     string
	deadLetter string
}

// NewStream creates a new instance of Stream.
func NewStream(client *redis.Client, stream string) *Stream {
	if client == nil {
		return nil
	}

	return &Stream{
		client:     client,
		stream:     stream,
		deadLetter: fmt.Sprintf("%s:dead-letter", stream),
	}
}

// EnsureConsumerGroup ensures that a consumer group exists for the stream.
func (n *Stream) EnsureConsumerGroup(ctx context.Context, group string) error {
	err := n.client.XGroupCreateMkStream(ctx, n.stream, group, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return err
	}
	return nil
}

// Publish publishes a message to the stream.
func (n *Stream) Publish(ctx context.Context, msg any) error {
	payload, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	return n.client.XAdd(ctx, &redis.XAddArgs{
		Stream: n.stream,
		Values: map[string]any{"payload": payload},
	}).Err()
}

// ReadGroup reads messages from a stream using a consumer group.
func (n *Stream) ReadGroup(ctx context.Context, group, consumer string, count int64, block time.Duration) ([]redis.XMessage, error) {
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

// Ack acknowledges messages in a consumer group.
func (n *Stream) Ack(ctx context.Context, group string, ids ...string) error {
	return n.client.XAck(ctx, n.stream, group, ids...).Err()
}

// AutoClaim automatically claims pending messages from other consumers.
func (n *Stream) AutoClaim(ctx context.Context, group, consumer string, minIdle time.Duration, start string, count int64) (msgs []redis.XMessage, next string, err error) {
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

// PublishDeadLetter publishes a message to the dead-letter stream.
func (n *Stream) PublishDeadLetter(ctx context.Context, msg any) error {
	payload, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("dead-letter marshal: %w", err)
	}

	return n.client.XAdd(ctx, &redis.XAddArgs{
		Stream: n.deadLetter,
		Values: map[string]any{"payload": payload},
	}).Err()
}
