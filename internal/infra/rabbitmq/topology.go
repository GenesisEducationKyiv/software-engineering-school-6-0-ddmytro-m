// Package rabbitmq provides connection, topology, publishing, and consuming
// for the github-scanner broker.
package rabbitmq

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	// EventsExchange is the topic exchange for domain events.
	EventsExchange = "github_scanner.events"
	// CommandsExchange is the topic exchange for delivery commands.
	CommandsExchange = "github_scanner.commands"

	// RoutingKeyReleaseDetected routes a new-release event.
	RoutingKeyReleaseDetected = "release.detected"
	// RoutingKeyRepositoryMoved routes a repository-moved event.
	RoutingKeyRepositoryMoved = "repository.moved"
	// RoutingKeySubscriptionCreated routes a subscription-created event.
	RoutingKeySubscriptionCreated = "subscription.created"
	// RoutingKeyVerificationDelivered routes a verification-delivered result event.
	RoutingKeyVerificationDelivered = "verification.delivered"
	// RoutingKeyVerificationFailed routes a verification-failed result event.
	RoutingKeyVerificationFailed = "verification.failed"
	// RoutingKeyEmailSend routes an email-send command.
	RoutingKeyEmailSend = "email.send"

	// HeaderAttempts tracks the retry attempt count.
	HeaderAttempts = "x-attempts"
	// HeaderDLQReason records why a message was dead-lettered.
	HeaderDLQReason = "x-dlq-reason"
)

// QueueSet is the main/retry/dlq trio backing one reliable consumer.
type QueueSet struct {
	Main  string
	Retry string
	DLQ   string
}

// Endpoint binds a queue set to an exchange via routing keys.
type Endpoint struct {
	Exchange    string
	RoutingKeys []string
	Queues      QueueSet
}

// NotificationsEndpoint is consumed by the notifier service.
var NotificationsEndpoint = Endpoint{
	Exchange: EventsExchange,
	RoutingKeys: []string{
		RoutingKeyReleaseDetected,
		RoutingKeyRepositoryMoved,
		RoutingKeySubscriptionCreated,
	},
	Queues: QueueSet{
		Main:  "notifications",
		Retry: "notifications.retry",
		DLQ:   "notifications.dlq",
	},
}

// CommandsEndpoint is consumed by the mailer service.
var CommandsEndpoint = Endpoint{
	Exchange:    CommandsExchange,
	RoutingKeys: []string{RoutingKeyEmailSend},
	Queues: QueueSet{
		Main:  "email.delivery",
		Retry: "email.delivery.retry",
		DLQ:   "email.delivery.dlq",
	},
}

var exchanges = []string{EventsExchange, CommandsExchange}

var endpoints = []Endpoint{NotificationsEndpoint, CommandsEndpoint}

func declareTopology(ch *amqp.Channel, retry RetryPolicy) error {
	for _, ex := range exchanges {
		if err := ch.ExchangeDeclare(ex, "topic", true, false, false, false, nil); err != nil {
			return fmt.Errorf("declare exchange %s: %w", ex, err)
		}
	}
	for _, ep := range endpoints {
		if err := declareEndpoint(ch, ep, retry); err != nil {
			return err
		}
	}
	return nil
}

func declareEndpoint(ch *amqp.Channel, ep Endpoint, retry RetryPolicy) error {
	if _, err := ch.QueueDeclare(ep.Queues.Main, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare %s queue: %w", ep.Queues.Main, err)
	}
	for _, rk := range ep.RoutingKeys {
		if err := ch.QueueBind(ep.Queues.Main, rk, ep.Exchange, false, nil); err != nil {
			return fmt.Errorf("bind %s to %s: %w", rk, ep.Queues.Main, err)
		}
	}

	// One wait queue per backoff tier: each holds messages for an exponentially
	// larger delay, then dead-letters them back to the main queue via the
	// default exchange (consumers dispatch on the body, not the routing key).
	for tier := range retry.Tiers {
		args := amqp.Table{
			"x-dead-letter-exchange":    "",
			"x-dead-letter-routing-key": ep.Queues.Main,
			"x-message-ttl":             retry.tierTTLms(tier),
		}
		name := retryQueueName(ep.Queues.Retry, tier)
		if _, err := ch.QueueDeclare(name, true, false, false, false, args); err != nil {
			return fmt.Errorf("declare %s queue: %w", name, err)
		}
	}

	if _, err := ch.QueueDeclare(ep.Queues.DLQ, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare %s queue: %w", ep.Queues.DLQ, err)
	}
	return nil
}
