package mq

import (
	"context"
	"time"
)

// Message represents a single message retrieved from the message queue.
type Message interface {
	// ID returns the unique identifier of the message.
	ID() string
	// Payload returns the raw byte content of the message.
	Payload() []byte
}

// Stream defines the interface for interacting with a message stream.
type Stream interface {
	// Ack acknowledges that one or more messages have been processed.
	Ack(ctx context.Context, group string, ids ...string) error
	// AutoClaim transfers ownership of pending messages that have been idle for a specific duration.
	AutoClaim(ctx context.Context, group, consumer string, minIdle time.Duration, start string, count int64) (msgs []Message, next string, err error)
	// EnsureConsumerGroup creates the consumer group if it does not already exist.
	EnsureConsumerGroup(ctx context.Context, group string) error
	// PublishDeadLetter moves a failed message to a dead-letter queue for manual inspection.
	PublishDeadLetter(ctx context.Context, msg any) error
	// ReadGroup reads new messages from the stream for a specific consumer group.
	ReadGroup(ctx context.Context, group, consumer string, count int64, block time.Duration) ([]Message, error)
}
