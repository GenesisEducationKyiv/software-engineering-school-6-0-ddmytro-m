// Package outbox persists domain events durably alongside the DB writes
// that produce them, and relays them to the broker asynchronously. This
// keeps a state change and the notifications it implies atomic even when
// RabbitMQ is briefly unavailable.
package outbox

import (
	"encoding/json"

	"gorm.io/gorm"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/events"
)

// Row is a durably persisted event pending publish to the broker.
type Row struct {
	gorm.Model
	RoutingKey string
	Payload    []byte `gorm:"type:jsonb;not null"`
}

// TableName overrides GORM's default pluralized "rows", which is too
// generic and collision-prone for a shared migration namespace.
func (Row) TableName() string {
	return "outbox_rows"
}

// Event is an envelope queued for durable publish via the outbox.
type Event struct {
	RoutingKey string
	Envelope   events.Envelope
}

// New builds an Event from an envelope constructor's result, propagating any
// construction error so callers can write outbox.New(events.NewFoo(...)).
func New(env events.Envelope, err error) (Event, error) {
	if err != nil {
		return Event{}, err
	}
	return Event{RoutingKey: string(env.Type), Envelope: env}, nil
}

// InsertTx persists events using the caller's transaction, so they commit
// atomically with whatever else that transaction is doing. It is a no-op
// for zero events.
func InsertTx(tx *gorm.DB, evts ...Event) error {
	if len(evts) == 0 {
		return nil
	}
	rows := make([]Row, 0, len(evts))
	for _, e := range evts {
		payload, err := json.Marshal(e.Envelope)
		if err != nil {
			return err
		}
		rows = append(rows, Row{RoutingKey: e.RoutingKey, Payload: payload})
	}
	return tx.Create(&rows).Error
}
