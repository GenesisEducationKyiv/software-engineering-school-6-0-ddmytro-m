package orchestrator

import (
	"context"
	"errors"
	"time"

	"gorm.io/gorm"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/db"
)

// ErrAlreadySettled means the saga already left awaiting_delivery - the
// orchestrator and the Reaper raced and the other side won. Not a failure.
var ErrAlreadySettled = errors.New("saga already settled")

// Store looks up and settles onboarding-saga state and runs the subscription
// compensation. Saga creation lives in handlers.SubscriptionRepository, in the
// same transaction as the subscription write and its outbox event.
type Store interface {
	SagaState(ctx context.Context, token string) (db.SagaState, error)
	// MarkCompleted transitions the saga to completed, or ErrAlreadySettled.
	MarkCompleted(ctx context.Context, token string) error
	// Compensate cancels the pending subscription and marks the saga
	// compensated, atomically, or returns ErrAlreadySettled.
	Compensate(ctx context.Context, token string) error
	// StaleAwaitingTokens returns confirm tokens of sagas still
	// awaiting_delivery after olderThan, for the reaper to compensate.
	StaleAwaitingTokens(ctx context.Context, olderThan time.Duration) ([]string, error)
}

type gormStore struct {
	db *gorm.DB
}

// NewStore creates a GORM-backed saga Store.
func NewStore(database *gorm.DB) Store {
	return &gormStore{db: database}
}

func (s *gormStore) SagaState(ctx context.Context, token string) (db.SagaState, error) {
	var saga db.OnboardingSaga
	if err := s.db.WithContext(ctx).Where("confirm_token = ?", token).First(&saga).Error; err != nil {
		return "", err
	}
	return saga.State, nil
}

// MarkCompleted only transitions rows still awaiting_delivery, so it can't
// race with a concurrent Reaper compensation: whichever write lands first
// wins, the other is a no-op reported as ErrAlreadySettled.
func (s *gormStore) MarkCompleted(ctx context.Context, token string) error {
	res := s.db.WithContext(ctx).Model(&db.OnboardingSaga{}).
		Where("confirm_token = ? AND state = ?", token, db.SagaAwaitingDelivery).
		Update("state", db.SagaCompleted)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return ErrAlreadySettled
	}
	return nil
}

// Compensate soft-deletes the still-pending subscription and marks the saga
// compensated in one transaction, so a crash between the two never leaves
// them inconsistent. The saga transition only applies to rows still
// awaiting_delivery, so it can't clobber a completion that raced ahead of it
// (see MarkCompleted); when that happens the subscription is left untouched.
func (s *gormStore) Compensate(ctx context.Context, token string) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		res := tx.Model(&db.OnboardingSaga{}).
			Where("confirm_token = ? AND state = ?", token, db.SagaAwaitingDelivery).
			Update("state", db.SagaCompensated)
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected == 0 {
			return ErrAlreadySettled
		}
		return tx.
			Where("confirm_token = ? AND status = ?", token, db.StatusPending).
			Delete(&db.Subscription{}).Error
	})
}

// StaleAwaitingTokens returns confirm tokens of sagas that have sat in
// awaiting_delivery since before olderThan ago - i.e. the saga-start or the
// mailer's result event was lost.
func (s *gormStore) StaleAwaitingTokens(ctx context.Context, olderThan time.Duration) ([]string, error) {
	var tokens []string
	err := s.db.WithContext(ctx).Model(&db.OnboardingSaga{}).
		Where("state = ? AND created_at < ?", db.SagaAwaitingDelivery, time.Now().Add(-olderThan)).
		Pluck("confirm_token", &tokens).Error
	return tokens, err
}
