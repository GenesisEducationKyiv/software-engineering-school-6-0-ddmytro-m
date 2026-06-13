package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.uber.org/zap"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/logger"
)

const publishConfirmTimeout = 5 * time.Second

// Publisher publishes messages to the RabbitMQ topic exchange with publisher confirms.
type Publisher struct {
	conn *Connection
}

// NewPublisher creates a Publisher backed by the given Connection.
func NewPublisher(conn *Connection) *Publisher {
	return &Publisher{conn: conn}
}

// Publish serialises msg as JSON and publishes it to the exchange with the
// given routing key. It waits for a publisher confirm and returns an error on
// nack, timeout, or channel failure.
func (p *Publisher) Publish(ctx context.Context, routingKey string, msg any) error {
	ch, err := p.conn.Channel()
	if err != nil {
		return fmt.Errorf("rabbitmq publish: open channel: %w", err)
	}
	defer func() {
		if closeErr := ch.Close(); closeErr != nil {
			logger.Log.Error("rabbitmq publisher: error closing channel", zap.Error(closeErr))
		}
	}()

	if err = ch.Confirm(false); err != nil {
		return fmt.Errorf("rabbitmq publish: enable confirms: %w", err)
	}

	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))

	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("rabbitmq publish: marshal: %w", err)
	}

	if err = ch.PublishWithContext(ctx, Exchange, routingKey, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         body,
	}); err != nil {
		return fmt.Errorf("rabbitmq publish: %w", err)
	}

	select {
	case confirm, ok := <-confirms:
		if !ok {
			return fmt.Errorf("rabbitmq publish: confirms channel closed")
		}
		if !confirm.Ack {
			return fmt.Errorf("rabbitmq publish: nack received for routing key %s", routingKey)
		}
		return nil
	case <-time.After(publishConfirmTimeout):
		return fmt.Errorf("rabbitmq publish: confirm timeout for routing key %s", routingKey)
	case <-ctx.Done():
		return ctx.Err()
	}
}
