package mailer

import (
	"context"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/events"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/rabbitmq"
)

// brokerPublisher publishes to an exchange with a routing key.
type brokerPublisher interface {
	Publish(ctx context.Context, exchange, routingKey string, msg any) error
}

type resultPublisher struct {
	broker brokerPublisher
}

// NewResultPublisher publishes onboarding-saga result events to the events exchange.
func NewResultPublisher(broker brokerPublisher) ResultPublisher {
	return &resultPublisher{broker: broker}
}

func (p *resultPublisher) VerificationDelivered(ctx context.Context, token string) error {
	env, err := events.NewVerificationDelivered(events.VerificationDelivered{Token: token})
	if err != nil {
		return err
	}
	return p.broker.Publish(ctx, rabbitmq.EventsExchange, rabbitmq.RoutingKeyVerificationDelivered, env)
}

func (p *resultPublisher) VerificationFailed(ctx context.Context, token, reason string) error {
	env, err := events.NewVerificationFailed(events.VerificationFailed{Token: token, Reason: reason})
	if err != nil {
		return err
	}
	return p.broker.Publish(ctx, rabbitmq.EventsExchange, rabbitmq.RoutingKeyVerificationFailed, env)
}
