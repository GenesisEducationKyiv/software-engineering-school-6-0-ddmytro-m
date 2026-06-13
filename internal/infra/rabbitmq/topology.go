// Package rabbitmq provides RabbitMQ connection management, topology declaration,
// publishing, and consuming for the github-scanner event broker.
package rabbitmq

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	// Exchange is the durable topic exchange for domain events.
	Exchange = "github_scanner.events"

	// QueueNotifications is the primary consumer queue.
	QueueNotifications = "notifications"

	// QueueRetry is the wait queue used for retrying failed messages.
	QueueRetry = "notifications.retry"

	// QueueDLQ is the dead-letter queue for exhausted / poison messages.
	QueueDLQ = "notifications.dlq"

	// RoutingKeyReleaseDetected is published when a new release is found.
	RoutingKeyReleaseDetected = "release.detected"

	// RoutingKeyRepositoryMoved is published when a repository ID mismatch is detected.
	RoutingKeyRepositoryMoved = "repository.moved"

	// RoutingKeySubscriptionCreated is published when a subscription is created.
	RoutingKeySubscriptionCreated = "subscription.created"

	// HeaderAttempts is the header used to track the retry attempt count.
	HeaderAttempts = "x-attempts"
)

// declareTopology idempotently creates the exchange, queues, and bindings
// required by the notification pipeline. It is called after each reconnect.
func declareTopology(ch *amqp.Channel, retryTTLMs int64) error {
	// Durable topic exchange
	if err := ch.ExchangeDeclare(
		Exchange,
		"topic",
		true,  // durable
		false, // auto-delete
		false, // internal
		false, // no-wait
		nil,
	); err != nil {
		return fmt.Errorf("declare exchange: %w", err)
	}

	// Main notifications queue
	if _, err := ch.QueueDeclare(
		QueueNotifications,
		true,  // durable
		false, // auto-delete
		false, // exclusive
		false, // no-wait
		nil,
	); err != nil {
		return fmt.Errorf("declare %s queue: %w", QueueNotifications, err)
	}

	// Bind the main queue to the exchange for all three routing keys
	for _, rk := range []string{
		RoutingKeyReleaseDetected,
		RoutingKeyRepositoryMoved,
		RoutingKeySubscriptionCreated,
	} {
		if err := ch.QueueBind(QueueNotifications, rk, Exchange, false, nil); err != nil {
			return fmt.Errorf("bind %s to %s: %w", rk, QueueNotifications, err)
		}
	}

	// Retry wait queue: messages expire after retryTTLMs and are dead-lettered
	// back to the main exchange (same routing key is preserved).
	retryArgs := amqp.Table{
		"x-dead-letter-exchange": Exchange,
		"x-message-ttl":          retryTTLMs,
	}
	if _, err := ch.QueueDeclare(
		QueueRetry,
		true,  // durable
		false, // auto-delete
		false, // exclusive
		false, // no-wait
		retryArgs,
	); err != nil {
		return fmt.Errorf("declare %s queue: %w", QueueRetry, err)
	}

	// DLQ: final resting place for poison / exhausted messages
	if _, err := ch.QueueDeclare(
		QueueDLQ,
		true,  // durable
		false, // auto-delete
		false, // exclusive
		false, // no-wait
		nil,
	); err != nil {
		return fmt.Errorf("declare %s queue: %w", QueueDLQ, err)
	}

	return nil
}
