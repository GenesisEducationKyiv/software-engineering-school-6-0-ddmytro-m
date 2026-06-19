// Package orchestrator runs the subscription-onboarding saga: it starts the
// verification flow and settles or compensates it from the mailer's results.
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

// VerificationPublisher publishes the subscription.created event that drives the
// forward path. Satisfied by *eventpublisher.Publisher.
type VerificationPublisher interface {
	SendEmailVerification(email, token string) error
}

// settler settles a single result delivery with the broker.
type settler interface {
	Ack()
	Retry(ctx context.Context, reason string)
	DeadLetter(ctx context.Context, reason string)
}

// Orchestrator owns the onboarding saga state machine.
type Orchestrator struct {
	store     Store
	publisher VerificationPublisher
}

// New creates an Orchestrator.
func New(store Store, publisher VerificationPublisher) *Orchestrator {
	return &Orchestrator{store: store, publisher: publisher}
}

// SendEmailVerification starts the onboarding saga: it persists saga state and
// then publishes subscription.created. On publish failure the saga row is rolled
// back so the caller can surface the error and a retry starts cleanly. It
// satisfies the HTTP handler's EmailSender interface.
func (o *Orchestrator) SendEmailVerification(email, token string) error {
	if err := o.store.CreateSaga(token); err != nil {
		return fmt.Errorf("create saga: %w", err)
	}
	if err := o.publisher.SendEmailVerification(email, token); err != nil {
		if delErr := o.store.DeleteSaga(token); delErr != nil {
			logger.Log.Error("failed to roll back saga row after publish failure", zap.Error(delErr))
		}
		return err
	}
	return nil
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
	state, err := o.store.SagaState(token)
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
	if !o.claim(ctx, token, s) {
		return
	}
	if err := o.store.MarkCompleted(token); err != nil {
		s.Retry(ctx, "mark completed: "+err.Error())
		return
	}
	logger.Log.Info("onboarding saga completed", zap.String("token", token))
	s.Ack()
}

// compensate runs C1: cancel the still-pending subscription, then mark the saga
// compensated. Both steps are idempotent so a redelivery is safe.
func (o *Orchestrator) compensate(ctx context.Context, token string, s settler) {
	if !o.claim(ctx, token, s) {
		return
	}
	if err := o.store.CancelPendingSubscription(token); err != nil {
		s.Retry(ctx, "cancel subscription: "+err.Error())
		return
	}
	if err := o.store.MarkCompensated(token); err != nil {
		s.Retry(ctx, "mark compensated: "+err.Error())
		return
	}
	logger.Log.Info("onboarding saga compensated", zap.String("token", token))
	s.Ack()
}
