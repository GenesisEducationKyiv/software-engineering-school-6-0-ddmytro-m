package orchestrator

import (
	"gorm.io/gorm"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/db"
)

// Store persists onboarding-saga state and runs the subscription compensation.
type Store interface {
	CreateSaga(token string) error
	DeleteSaga(token string) error
	SagaState(token string) (db.SagaState, error)
	MarkCompleted(token string) error
	MarkCompensated(token string) error
	CancelPendingSubscription(token string) error
}

type gormStore struct {
	db *gorm.DB
}

// NewStore creates a GORM-backed saga Store.
func NewStore(database *gorm.DB) Store {
	return &gormStore{db: database}
}

func (s *gormStore) CreateSaga(token string) error {
	return s.db.Create(&db.OnboardingSaga{ConfirmToken: token, State: db.SagaAwaitingDelivery}).Error
}

func (s *gormStore) DeleteSaga(token string) error {
	return s.db.Where("confirm_token = ?", token).Delete(&db.OnboardingSaga{}).Error
}

func (s *gormStore) SagaState(token string) (db.SagaState, error) {
	var saga db.OnboardingSaga
	if err := s.db.Where("confirm_token = ?", token).First(&saga).Error; err != nil {
		return "", err
	}
	return saga.State, nil
}

func (s *gormStore) MarkCompleted(token string) error {
	return s.updateState(token, db.SagaCompleted)
}

func (s *gormStore) MarkCompensated(token string) error {
	return s.updateState(token, db.SagaCompensated)
}

func (s *gormStore) updateState(token string, state db.SagaState) error {
	return s.db.Model(&db.OnboardingSaga{}).
		Where("confirm_token = ?", token).
		Update("state", state).Error
}

// CancelPendingSubscription soft-deletes the subscription for the given confirm
// token only while it is still pending; a confirmed subscription is left intact.
func (s *gormStore) CancelPendingSubscription(token string) error {
	return s.db.
		Where("confirm_token = ? AND status = ?", token, db.StatusPending).
		Delete(&db.Subscription{}).Error
}
