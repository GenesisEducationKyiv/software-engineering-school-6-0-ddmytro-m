// Package orchestrator settles the subscription-onboarding saga from the
// mailer's delivery results. Saga-start is not this package's concern: it
// happens transactionally alongside the subscription write and its outbox
// event (see handlers.SubscriptionRepository), so a saga only ever exists
// once that transaction has durably committed.
package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/events"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/db"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/logger"
)

// settler settles a single result delivery with the broker.
type settler interface {
	Ack()
	Retry(ctx context.Context, reason string)
	DeadLetter(ctx context.Context, reason string)
}

// Orchestrator owns the onboarding saga state machine.
type Orchestrator struct {
	store Store
}

// New creates an Orchestrator.
func New(store Store) *Orchestrator {
	return &Orchestrator{store: store}
}

// process handles one result event and settles it.
func (o *Orchestrator) process(ctx context.Context, body []byte, s settler) {
	var env events.Envelope
	if err := json.Unmarshal(body, &env); err != nil {
		s.DeadLetter(ctx, "unmarshal envelope: "+err.Error())
		return
	}

	switch env.Type {
	case events.TypeVerificationDelivered:
		p, err := env.DecodeVerificationDelivered()
		if err != nil {
			s.DeadLetter(ctx, "decode verification.delivered: "+err.Error())
			return
		}
		o.complete(ctx, p.Token, s)
	case events.TypeVerificationFailed:
		p, err := env.DecodeVerificationFailed()
		if err != nil {
			s.DeadLetter(ctx, "decode verification.failed: "+err.Error())
			return
		}
		o.compensate(ctx, p.Token, s)
	default:
		s.DeadLetter(ctx, fmt.Sprintf("unexpected event type: %q", env.Type))
	}
}

// claim reports whether the saga for token is open and should be acted on. It
// settles (ack) and returns false for a missing or already-terminal saga,
// making result handling idempotent under at-least-once redelivery.
func (o *Orchestrator) claim(ctx context.Context, token string, s settler) bool {
	state, err := o.store.SagaState(ctx, token)
	switch {
	case errors.Is(err, gorm.ErrRecordNotFound):
		// No saga (e.g. a lost start); nothing to settle.
		logger.Log.Warn("result for unknown saga, acking", zap.String("token", token))
		s.Ack()
		return false
	case err != nil:
		s.Retry(ctx, "saga lookup: "+err.Error())
		return false
	case state != db.SagaAwaitingDelivery:
		// Already completed or compensated.
		s.Ack()
		return false
	}
	return true
}

// complete marks the saga delivered.
func (o *Orchestrator) complete(ctx context.Context, token string, s settler) {
	o.settle(ctx, token, s, o.store.MarkCompleted, settleLog{
		race:    "saga completion raced with reaper compensation, acking",
		success: "onboarding saga completed",
		retry:   "mark completed: ",
	})
}

// compensate runs C1: cancel the still-pending subscription and mark the saga
// compensated, atomically. The whole operation is also idempotent, so a
// redelivery after a retry is safe.
func (o *Orchestrator) compensate(ctx context.Context, token string, s settler) {
	o.settle(ctx, token, s, o.store.Compensate, settleLog{
		race:    "saga compensation raced with completion, acking",
		success: "onboarding saga compensated",
		retry:   "compensate: ",
	})
}

// settleLog carries the log/retry messages that distinguish complete from
// compensate in settle.
type settleLog struct {
	race, success, retry string
}

// settle runs a claimed saga's settling write (MarkCompleted or Compensate)
// and acks or retries the result. ErrAlreadySettled means the other side of
// the saga (reaper vs. orchestrator) already settled it, so it's acked, not
// retried.
func (o *Orchestrator) settle(ctx context.Context, token string, s settler, write func(context.Context, string) error, msg settleLog) {
	if !o.claim(ctx, token, s) {
		return
	}
	if err := write(ctx, token); err != nil {
		if errors.Is(err, ErrAlreadySettled) {
			logger.Log.Warn(msg.race, zap.String("token", token))
			s.Ack()
			return
		}
		s.Retry(ctx, msg.retry+err.Error())
		return
	}
	logger.Log.Info(msg.success, zap.String("token", token))
	s.Ack()
}
