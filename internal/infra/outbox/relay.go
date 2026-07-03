package outbox

import (
	"context"
	"encoding/json"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/rabbitmq"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/logger"
)

// Broker publishes a message to an exchange with a routing key.
type Broker interface {
	Publish(ctx context.Context, exchange, routingKey string, msg any) error
}

// Relay polls the outbox table and publishes pending rows to the broker,
// deleting each row once its publish is confirmed.
type Relay struct {
	db           *gorm.DB
	broker       Broker
	pollInterval time.Duration
	batchSize    int
}

// NewRelay creates a Relay backed by the given DB and broker.
func NewRelay(db *gorm.DB, broker Broker, pollInterval time.Duration, batchSize int) *Relay {
	return &Relay{db: db, broker: broker, pollInterval: pollInterval, batchSize: batchSize}
}

// Run polls and delivers pending rows until ctx is cancelled.
func (r *Relay) Run(ctx context.Context) {
	r.deliver(ctx)

	ticker := time.NewTicker(r.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.deliver(ctx)
		}
	}
}

// deliver drains the table in FIFO batches, stopping at the first publish
// failure so the next tick retries from the same row and delivery order is
// preserved.
func (r *Relay) deliver(ctx context.Context) {
	for {
		var rows []Row
		if err := r.db.Order("id").Limit(r.batchSize).Find(&rows).Error; err != nil {
			logger.Log.Error("outbox: query pending rows failed", zap.Error(err))
			return
		}
		if len(rows) == 0 {
			return
		}

		for _, row := range rows {
			if err := r.broker.Publish(ctx, rabbitmq.EventsExchange, row.RoutingKey, json.RawMessage(row.Payload)); err != nil {
				logger.Log.Error("outbox: publish failed, will retry", zap.Error(err), zap.String("routing_key", row.RoutingKey))
				return
			}
			if err := r.db.Delete(&row).Error; err != nil {
				logger.Log.Error("outbox: delete after publish failed, will republish", zap.Error(err), zap.String("routing_key", row.RoutingKey))
			}
		}

		if len(rows) < r.batchSize {
			return
		}
	}
}
