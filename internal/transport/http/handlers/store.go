package handlers

import (
	"context"

	"gorm.io/gorm"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/api/github"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/db"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/outbox"
)

// SubscriptionRepository defines data access methods for the subscription handler.
type SubscriptionRepository interface {
	FindRepoByPath(owner, name string) (*db.Repository, error)
	FindRepoByGitHubID(githubID int64) (*db.Repository, error)
	SaveRepo(repo *db.Repository) error
	CreateRepo(repo *db.Repository) error
	FindSubscription(email string, repoID uint) (*db.Subscription, error)
	IsConfirmTokenTaken(token string) (bool, error)
	SaveSubscription(sub *db.Subscription, events ...outbox.Event) error
	CreateSubscription(sub *db.Subscription, events ...outbox.Event) error
	FindSubscriptionByConfirmToken(token string) (*db.Subscription, error)
	FindSubscriptionByTokens(confirmToken, apiToken string) (*db.Subscription, error)
	FindSubscriptionByEmailAndToken(email, apiToken string) (*db.Subscription, error)
	ListSubscriptions(email string) ([]db.Subscription, error)
	FindReposByIDs(ids []uint) ([]db.Repository, error)
}

// RepoResolver resolves GitHub repository metadata by owner and name.
type RepoResolver interface {
	GetRepository(ctx context.Context, owner, name, etag string) github.Response[github.Repository]
}

type gormSubscriptionStore struct {
	db *gorm.DB
}

// NewSubscriptionStore creates a new GORM-backed SubscriptionRepository.
func NewSubscriptionStore(db *gorm.DB) SubscriptionRepository {
	return &gormSubscriptionStore{db: db}
}

func (s *gormSubscriptionStore) FindRepoByPath(owner, name string) (*db.Repository, error) {
	var repo db.Repository
	if err := s.db.Where("owner = ? AND name = ?", owner, name).First(&repo).Error; err != nil {
		return nil, err
	}
	return &repo, nil
}

func (s *gormSubscriptionStore) FindRepoByGitHubID(githubID int64) (*db.Repository, error) {
	var repo db.Repository
	if err := s.db.Where("github_id = ?", githubID).First(&repo).Error; err != nil {
		return nil, err
	}
	return &repo, nil
}

func (s *gormSubscriptionStore) SaveRepo(repo *db.Repository) error {
	return s.db.Save(repo).Error
}

func (s *gormSubscriptionStore) CreateRepo(repo *db.Repository) error {
	return s.db.Create(repo).Error
}

func (s *gormSubscriptionStore) FindSubscription(email string, repoID uint) (*db.Subscription, error) {
	var sub db.Subscription
	if err := s.db.Where("email = ? AND repository_id = ?", email, repoID).First(&sub).Error; err != nil {
		return nil, err
	}
	return &sub, nil
}

func (s *gormSubscriptionStore) IsConfirmTokenTaken(token string) (bool, error) {
	var count int64
	if err := s.db.Model(&db.Subscription{}).Where("confirm_token = ?", token).Count(&count).Error; err != nil {
		return false, err
	}
	return count > 0, nil
}

func (s *gormSubscriptionStore) SaveSubscription(sub *db.Subscription, events ...outbox.Event) error {
	if len(events) == 0 {
		return s.db.Save(sub).Error
	}
	return s.writeSubscriptionTx(sub, events, func(tx *gorm.DB) error { return tx.Save(sub).Error })
}

func (s *gormSubscriptionStore) CreateSubscription(sub *db.Subscription, events ...outbox.Event) error {
	if len(events) == 0 {
		return s.db.Create(sub).Error
	}
	return s.writeSubscriptionTx(sub, events, func(tx *gorm.DB) error { return tx.Create(sub).Error })
}

// writeSubscriptionTx runs write, starts the onboarding saga that tracks this
// confirm token's verification-email outcome, and queues the outbox events,
// all in one transaction. They commit or roll back together, so a saga only
// ever exists once the subscription write has durably committed.
func (s *gormSubscriptionStore) writeSubscriptionTx(sub *db.Subscription, events []outbox.Event, write func(tx *gorm.DB) error) error {
	return s.db.Transaction(func(tx *gorm.DB) error {
		if err := write(tx); err != nil {
			return err
		}
		if err := tx.Create(&db.OnboardingSaga{ConfirmToken: sub.ConfirmToken, State: db.SagaAwaitingDelivery}).Error; err != nil {
			return err
		}
		return outbox.InsertTx(tx, events...)
	})
}

func (s *gormSubscriptionStore) FindSubscriptionByConfirmToken(token string) (*db.Subscription, error) {
	var sub db.Subscription
	if err := s.db.Where("confirm_token = ?", token).First(&sub).Error; err != nil {
		return nil, err
	}
	return &sub, nil
}

func (s *gormSubscriptionStore) FindSubscriptionByTokens(confirmToken, apiToken string) (*db.Subscription, error) {
	var sub db.Subscription
	if err := s.db.Where("confirm_token = ? AND api_token = ?", confirmToken, apiToken).First(&sub).Error; err != nil {
		return nil, err
	}
	return &sub, nil
}

func (s *gormSubscriptionStore) FindSubscriptionByEmailAndToken(email, apiToken string) (*db.Subscription, error) {
	var sub db.Subscription
	if err := s.db.Where("email = ? AND api_token = ?", email, apiToken).First(&sub).Error; err != nil {
		return nil, err
	}
	return &sub, nil
}

func (s *gormSubscriptionStore) ListSubscriptions(email string) ([]db.Subscription, error) {
	var subs []db.Subscription
	if err := s.db.Where("email = ? AND status != ?", email, db.StatusUnsubscribed).Find(&subs).Error; err != nil {
		return nil, err
	}
	return subs, nil
}

func (s *gormSubscriptionStore) FindReposByIDs(ids []uint) ([]db.Repository, error) {
	var repos []db.Repository
	if err := s.db.Where("id IN ?", ids).Find(&repos).Error; err != nil {
		return nil, err
	}
	return repos, nil
}
