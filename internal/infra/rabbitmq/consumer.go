package rabbitmq

import (
	"context"
	"fmt"
	"maps"
	"time"

	"go.uber.org/zap"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/logger"
)

// Settler settles a single delivery: exactly one of these must be called.
type Settler interface {
	Ack()
	Retry(ctx context.Context, reason string)
	DeadLetter(ctx context.Context, reason string)
}

// DeliveryHandler is invoked for each message; it must settle via s.
type DeliveryHandler func(ctx context.Context, body []byte, s Settler)

// settler binds a delivery to its consumer to satisfy Settler.
type settler struct {
	c *Consumer
	d amqp.Delivery
}

func (s settler) Ack()                                { s.c.Ack(s.d) }
func (s settler) Retry(ctx context.Context, r string) { s.c.Retry(ctx, s.d, r) }
func (s settler) DeadLetter(ctx context.Context, r string) {
	s.c.DeadLetter(ctx, s.d, r)
}

// Consumer consumes from a queue set with manual acks.
type Consumer struct {
	conn     *Connection
	queues   QueueSet
	prefetch int
	retry    RetryPolicy
	handler  DeliveryHandler
}

// NewConsumer creates a Consumer for the given queue set.
func NewConsumer(conn *Connection, queues QueueSet, prefetch int, retry RetryPolicy, handler DeliveryHandler) *Consumer {
	return &Consumer{
		conn:     conn,
		queues:   queues,
		prefetch: prefetch,
		retry:    retry,
		handler:  handler,
	}
}

// Start begins consuming messages and calls the handler for each one.
// It blocks until ctx is cancelled, reconnecting the channel as needed.
func (c *Consumer) Start(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		if err := c.consume(ctx); err != nil {
			logger.Log.Error("rabbitmq consumer: error, restarting", zap.Error(err))
		}
	}
}

func (c *Consumer) consume(ctx context.Context) error {
	ch, err := c.conn.Channel()
	if err != nil {
		return fmt.Errorf("open channel: %w", err)
	}
	defer func() {
		if closeErr := ch.Close(); closeErr != nil {
			logger.Log.Error("rabbitmq consumer: error closing channel", zap.Error(closeErr))
		}
	}()

	if err = ch.Qos(c.prefetch, 0, false); err != nil {
		return fmt.Errorf("set QoS: %w", err)
	}

	deliveries, err := ch.Consume(
		c.queues.Main,
		"",    // consumer tag (auto-generated)
		false, // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,
	)
	if err != nil {
		return fmt.Errorf("start consuming: %w", err)
	}

	logger.Log.Info("rabbitmq consumer: started")

	for {
		select {
		case <-ctx.Done():
			return nil
		case d, ok := <-deliveries:
			if !ok {
				return fmt.Errorf("deliveries channel closed")
			}
			c.handler(ctx, d.Body, settler{c: c, d: d})
		}
	}
}

// Ack acknowledges the delivery, signalling successful processing to the broker.
func (c *Consumer) Ack(d amqp.Delivery) {
	if err := d.Ack(false); err != nil {
		logger.Log.Error("rabbitmq consumer: ack failed", zap.Error(err))
	}
}

// attempts reads the current attempt count from the message headers, defaulting to 0.
func attempts(d amqp.Delivery) int64 {
	if d.Headers == nil {
		return 0
	}
	if v, ok := d.Headers[HeaderAttempts]; ok {
		switch val := v.(type) {
		case int32:
			return int64(val)
		case int64:
			return val
		}
	}
	return 0
}

// publishConfirmed publishes pub to queue via the default exchange and waits
// for a broker confirm before returning, so a caller that then acks the
// original delivery can trust the republish actually landed rather than
// being lost to a connection failure right after PublishWithContext returns.
func publishConfirmed(ctx context.Context, ch *amqp.Channel, queue string, pub amqp.Publishing) error {
	if err := ch.Confirm(false); err != nil {
		return fmt.Errorf("enable confirms: %w", err)
	}
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))

	if err := ch.PublishWithContext(ctx, "", queue, false, false, pub); err != nil {
		return fmt.Errorf("publish: %w", err)
	}

	select {
	case confirm, ok := <-confirms:
		if !ok {
			return fmt.Errorf("confirms channel closed")
		}
		if !confirm.Ack {
			return fmt.Errorf("nack received for queue %s", queue)
		}
		return nil
	case <-time.After(publishConfirmTimeout):
		return fmt.Errorf("confirm timeout for queue %s", queue)
	case <-ctx.Done():
		return ctx.Err()
	}
}

// nackRequeue nacks a delivery with requeue so the broker redelivers it,
// instead of leaving it unacked and stuck in-flight until the connection
// drops. Used on failure paths where we couldn't durably move the message
// forward (republish to a retry tier, or to the DLQ).
func nackRequeue(d amqp.Delivery) {
	if err := d.Nack(false, true); err != nil {
		logger.Log.Error("rabbitmq consumer: nack failed", zap.Error(err))
	}
}

// Retry republishes the message to the backoff tier matching its attempt count
// with an incremented x-attempts header, then acks the original. Once attempts
// exceed the configured tiers it calls DeadLetter instead.
func (c *Consumer) Retry(ctx context.Context, d amqp.Delivery, reason string) {
	att := attempts(d) + 1
	if att > int64(c.retry.Tiers) {
		c.DeadLetter(ctx, d, fmt.Sprintf("max retry attempts exceeded: %s", reason))
		return
	}
	tierQueue := retryQueueName(c.queues.Retry, int(att-1))

	ch, err := c.conn.Channel()
	if err != nil {
		logger.Log.Error("rabbitmq consumer: retry: open channel failed", zap.Error(err))
		nackRequeue(d)
		return
	}
	defer func() {
		if closeErr := ch.Close(); closeErr != nil {
			logger.Log.Error("rabbitmq consumer: retry: close channel failed", zap.Error(closeErr))
		}
	}()

	headers := amqp.Table{}
	maps.Copy(headers, d.Headers)
	headers[HeaderAttempts] = att

	pub := amqp.Publishing{
		ContentType:   d.ContentType,
		DeliveryMode:  amqp.Persistent,
		Body:          d.Body,
		Headers:       headers,
		CorrelationId: d.CorrelationId,
	}
	if pubErr := publishConfirmed(ctx, ch, tierQueue, pub); pubErr != nil {
		logger.Log.Error("rabbitmq consumer: retry: publish failed", zap.Error(pubErr), zap.Int64("attempt", att))
		nackRequeue(d)
		return
	}

	if ackErr := d.Ack(false); ackErr != nil {
		logger.Log.Error("rabbitmq consumer: retry: ack failed", zap.Error(ackErr))
	}

	logger.Log.Warn("rabbitmq consumer: message retried",
		zap.Int64("attempt", att),
		zap.Int("max", c.retry.Tiers),
		zap.String("reason", reason),
	)
}

// DeadLetter publishes the message to the DLQ with reason metadata, then acks
// the original.
func (c *Consumer) DeadLetter(ctx context.Context, d amqp.Delivery, reason string) {
	ch, err := c.conn.Channel()
	if err != nil {
		logger.Log.Error("rabbitmq consumer: dead-letter: open channel failed", zap.Error(err))
		nackRequeue(d)
		return
	}
	defer func() {
		if closeErr := ch.Close(); closeErr != nil {
			logger.Log.Error("rabbitmq consumer: dead-letter: close channel failed", zap.Error(closeErr))
		}
	}()

	headers := amqp.Table{}
	maps.Copy(headers, d.Headers)
	headers[HeaderDLQReason] = reason

	pub := amqp.Publishing{
		ContentType:  d.ContentType,
		DeliveryMode: amqp.Persistent,
		Body:         d.Body,
		Headers:      headers,
	}
	if pubErr := publishConfirmed(ctx, ch, c.queues.DLQ, pub); pubErr != nil {
		logger.Log.Error("rabbitmq consumer: dead-letter: publish failed", zap.Error(pubErr))
		nackRequeue(d)
		return
	}

	if ackErr := d.Ack(false); ackErr != nil {
		logger.Log.Error("rabbitmq consumer: dead-letter: ack failed", zap.Error(ackErr))
	}

	logger.Log.Warn("rabbitmq consumer: message dead-lettered", zap.String("reason", reason))
}
