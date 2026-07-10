package orchestrator

import (
	"context"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/rabbitmq"
)

// Handler adapts the orchestrator to a rabbitmq DeliveryHandler.
func (o *Orchestrator) Handler() rabbitmq.DeliveryHandler {
	return func(ctx context.Context, body []byte, s rabbitmq.Settler) {
		o.process(ctx, body, s)
	}
}
