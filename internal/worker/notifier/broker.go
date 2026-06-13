package notifier

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"

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

// amqpSettler settles a delivery through the rabbitmq Consumer.
type amqpSettler struct {
	c *rabbitmq.Consumer
	d amqp.Delivery
}

func (s amqpSettler) Ack()                                { s.c.Ack(s.d) }
func (s amqpSettler) Retry(ctx context.Context, r string) { s.c.Retry(ctx, s.d, r) }
func (s amqpSettler) DeadLetter(ctx context.Context, r string) {
	s.c.DeadLetter(ctx, s.d, r)
}

// Handler adapts the notifier to a rabbitmq DeliveryHandler.
func (n *Notifier) Handler() rabbitmq.DeliveryHandler {
	return func(ctx context.Context, c *rabbitmq.Consumer, d amqp.Delivery) {
		n.process(ctx, d.Body, amqpSettler{c: c, d: d})
	}
}
