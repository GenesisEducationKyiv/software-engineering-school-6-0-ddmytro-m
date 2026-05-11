package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/mq"
)

// MessageAdapter adapts a Redis XMessage to the mq.Message interface.
type MessageAdapter struct {
	xmsg goredis.XMessage
}

// NewMessageAdapter creates a new MessageAdapter from a Redis XMessage.
func NewMessageAdapter(xmsg goredis.XMessage) MessageAdapter {
	return MessageAdapter{xmsg: xmsg}
}

// ID returns the unique identifier of the Redis stream message.
func (m MessageAdapter) ID() string {
	return m.xmsg.ID
}

// Payload returns the byte content stored in the "payload" field of the Redis message.
func (m MessageAdapter) Payload() []byte {
	if val, ok := m.xmsg.Values["payload"].(string); ok {
		return []byte(val)
	}
	return nil
}

// Stream represents a Redis Stream client wrapper.
type Stream struct {
	client     *goredis.Client
	stream     string
	deadLetter string
}

// NewStream creates a new instance of Stream.
func NewStream(client *goredis.Client, stream string) *Stream {
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
func (s *Stream) EnsureConsumerGroup(ctx context.Context, group string) error {
	err := s.client.XGroupCreateMkStream(ctx, s.stream, group, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return err
	}

	return nil
}

// Publish publishes a message to the stream.
func (s *Stream) Publish(ctx context.Context, msg any) error {
	payload, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	return s.client.XAdd(ctx, &goredis.XAddArgs{
		Stream: s.stream,
		Values: map[string]any{"payload": payload},
	}).Err()
}

// PublishDeadLetter publishes a message to the dead-letter stream.
func (s *Stream) PublishDeadLetter(ctx context.Context, msg any) error {
	payload, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("dead-letter marshal: %w", err)
	}

	return s.client.XAdd(ctx, &goredis.XAddArgs{
		Stream: s.deadLetter,
		Values: map[string]any{"payload": payload},
	}).Err()
}

// ReadGroup reads messages from a stream using a consumer group.
func (s *Stream) ReadGroup(ctx context.Context, group, consumer string, count int64, block time.Duration) ([]mq.Message, error) {
	res, err := s.client.XReadGroup(ctx, &goredis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  []string{s.stream, ">"},
		Count:    count,
		Block:    block,
	}).Result()

	if err != nil {
		if errors.Is(err, goredis.Nil) {
			return nil, nil
		}
		return nil, err
	}
	if len(res) == 0 {
		return nil, nil
	}

	msgs := make([]mq.Message, len(res[0].Messages))
	for i, xmsg := range res[0].Messages {
		msgs[i] = &MessageAdapter{xmsg: xmsg}
	}

	return msgs, nil
}

// Ack acknowledges messages in a consumer group.
func (s *Stream) Ack(ctx context.Context, group string, ids ...string) error {
	return s.client.XAck(ctx, s.stream, group, ids...).Err()
}

// AutoClaim automatically claims pending messages from other consumers.
func (s *Stream) AutoClaim(ctx context.Context, group, consumer string, minIdle time.Duration, start string, count int64) (msgs []mq.Message, next string, err error) {
	xmsgs, next, err := s.client.XAutoClaim(ctx, &goredis.XAutoClaimArgs{
		Stream:   s.stream,
		Group:    group,
		Consumer: consumer,
		MinIdle:  minIdle,
		Start:    start,
		Count:    count,
	}).Result()

	if err != nil {
		return nil, "", err
	}
	if len(xmsgs) == 0 {
		return nil, "", nil
	}

	msgs = make([]mq.Message, len(xmsgs))
	for i, xmsg := range xmsgs {
		msgs[i] = &MessageAdapter{xmsg: xmsg}
	}

	return msgs, next, err
}
