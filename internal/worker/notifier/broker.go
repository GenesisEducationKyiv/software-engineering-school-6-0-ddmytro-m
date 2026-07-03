package notifier

import (
	"context"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/mq"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/rabbitmq"
)

// brokerPublisher publishes to an exchange with a routing key.
type brokerPublisher interface {
	Publish(ctx context.Context, exchange, routingKey string, msg any) error
}

type commandPublisher struct {
	broker brokerPublisher
}

// NewCommandPublisher publishes email commands to the commands exchange.
func NewCommandPublisher(broker brokerPublisher) CommandPublisher {
	return &commandPublisher{broker: broker}
}

func (p *commandPublisher) Publish(ctx context.Context, cmd mq.DeliveryMessage) error {
	return p.broker.Publish(ctx, rabbitmq.CommandsExchange, rabbitmq.RoutingKeyEmailSend, cmd)
}

// Handler adapts the notifier to a rabbitmq DeliveryHandler.
func (n *Notifier) Handler() rabbitmq.DeliveryHandler {
	return func(ctx context.Context, body []byte, s rabbitmq.Settler) {
		n.process(ctx, body, s)
	}
}
