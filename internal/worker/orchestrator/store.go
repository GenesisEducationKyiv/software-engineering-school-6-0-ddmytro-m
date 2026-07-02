package orchestrator

import (
	"time"

	"gorm.io/gorm"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/db"
)

// Store looks up and settles onboarding-saga state and runs the subscription
// compensation. Saga creation lives in handlers.SubscriptionRepository, in the
// same transaction as the subscription write and its outbox event.
type Store interface {
	SagaState(token string) (db.SagaState, error)
	MarkCompleted(token string) error
	// Compensate cancels the still-pending subscription for token and marks
	// the saga compensated, atomically.
	Compensate(token string) error
	// StaleAwaitingTokens returns confirm tokens of sagas still
	// awaiting_delivery after olderThan, for the reaper to compensate.
	StaleAwaitingTokens(olderThan time.Duration) ([]string, error)
}

type gormStore struct {
	db *gorm.DB
}

// NewStore creates a GORM-backed saga Store.
func NewStore(database *gorm.DB) Store {
	return &gormStore{db: database}
}

func (s *gormStore) SagaState(token string) (db.SagaState, error) {
	var saga db.OnboardingSaga
	if err := s.db.Where("confirm_token = ?", token).First(&saga).Error; err != nil {
		return "", err
	}
	return saga.State, nil
}

func (s *gormStore) MarkCompleted(token string) error {
	return s.db.Model(&db.OnboardingSaga{}).
		Where("confirm_token = ?", token).
		Update("state", db.SagaCompleted).Error
}

// Compensate soft-deletes the subscription for the given confirm token (only
// while it is still pending; a confirmed subscription is left intact) and
// marks the saga compensated in one transaction, so a crash between the two
// can never leave the subscription cancelled with the saga still
// awaiting_delivery, or vice versa.
func (s *gormStore) Compensate(token string) error {
	return s.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.
			Where("confirm_token = ? AND status = ?", token, db.StatusPending).
			Delete(&db.Subscription{}).Error; err != nil {
			return err
		}
		return tx.Model(&db.OnboardingSaga{}).
			Where("confirm_token = ?", token).
			Update("state", db.SagaCompensated).Error
	})
}

// StaleAwaitingTokens returns confirm tokens of sagas that have sat in
// awaiting_delivery since before olderThan ago - i.e. the saga-start or the
// mailer's result event was lost.
func (s *gormStore) StaleAwaitingTokens(olderThan time.Duration) ([]string, error) {
	var tokens []string
	err := s.db.Model(&db.OnboardingSaga{}).
		Where("state = ? AND created_at < ?", db.SagaAwaitingDelivery, time.Now().Add(-olderThan)).
		Pluck("confirm_token", &tokens).Error
	return tokens, err
}
