package db

import (
	"errors"

	"gorm.io/gorm"
)

// ErrNotFound is returned by the store when a record does not exist. It hides
// gorm.ErrRecordNotFound so callers don't depend on the ORM.
var ErrNotFound = errors.New("record not found")

// wrapNotFound translates the ORM's not-found error into the package sentinel.
func wrapNotFound(err error) error {
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return ErrNotFound
	}
	return err
}

// SubscriptionStore is a GORM-backed store for repositories and subscriptions.
type SubscriptionStore struct {
	db *gorm.DB
}

// NewSubscriptionStore creates a new GORM-backed subscription store.
func NewSubscriptionStore(db *gorm.DB) *SubscriptionStore {
	return &SubscriptionStore{db: db}
}

// FindRepoByPath returns the repository with the given owner and name.
func (s *SubscriptionStore) FindRepoByPath(owner, name string) (*Repository, error) {
	var repo Repository
	if err := s.db.Where("owner = ? AND name = ?", owner, name).First(&repo).Error; err != nil {
		return nil, wrapNotFound(err)
	}
	return &repo, nil
}

// FindRepoByGitHubID returns the repository with the given GitHub ID.
func (s *SubscriptionStore) FindRepoByGitHubID(githubID int64) (*Repository, error) {
	var repo Repository
	if err := s.db.Where("github_id = ?", githubID).First(&repo).Error; err != nil {
		return nil, wrapNotFound(err)
	}
	return &repo, nil
}

// SaveRepo persists changes to an existing repository.
func (s *SubscriptionStore) SaveRepo(repo *Repository) error {
	return s.db.Save(repo).Error
}

// CreateRepo inserts a new repository.
func (s *SubscriptionStore) CreateRepo(repo *Repository) error {
	return s.db.Create(repo).Error
}

// FindSubscription returns the subscription for the given email and repository.
func (s *SubscriptionStore) FindSubscription(email string, repoID uint) (*Subscription, error) {
	var sub Subscription
	if err := s.db.Where("email = ? AND repository_id = ?", email, repoID).First(&sub).Error; err != nil {
		return nil, wrapNotFound(err)
	}
	return &sub, nil
}

// IsConfirmTokenTaken reports whether a confirm token is already in use.
func (s *SubscriptionStore) IsConfirmTokenTaken(token string) (bool, error) {
	var count int64
	if err := s.db.Model(&Subscription{}).Where("confirm_token = ?", token).Count(&count).Error; err != nil {
		return false, err
	}
	return count > 0, nil
}

// SaveSubscription persists changes to an existing subscription.
func (s *SubscriptionStore) SaveSubscription(sub *Subscription) error {
	return s.db.Save(sub).Error
}

// CreateSubscription inserts a new subscription.
func (s *SubscriptionStore) CreateSubscription(sub *Subscription) error {
	return s.db.Create(sub).Error
}

// FindSubscriptionByConfirmToken returns the subscription with the given confirm token.
func (s *SubscriptionStore) FindSubscriptionByConfirmToken(token string) (*Subscription, error) {
	var sub Subscription
	if err := s.db.Where("confirm_token = ?", token).First(&sub).Error; err != nil {
		return nil, wrapNotFound(err)
	}
	return &sub, nil
}

// FindSubscriptionByTokens returns the subscription matching both tokens.
func (s *SubscriptionStore) FindSubscriptionByTokens(confirmToken, apiToken string) (*Subscription, error) {
	var sub Subscription
	if err := s.db.Where("confirm_token = ? AND api_token = ?", confirmToken, apiToken).First(&sub).Error; err != nil {
		return nil, wrapNotFound(err)
	}
	return &sub, nil
}

// FindSubscriptionByEmailAndToken returns the subscription matching the email and API token.
func (s *SubscriptionStore) FindSubscriptionByEmailAndToken(email, apiToken string) (*Subscription, error) {
	var sub Subscription
	if err := s.db.Where("email = ? AND api_token = ?", email, apiToken).First(&sub).Error; err != nil {
		return nil, wrapNotFound(err)
	}
	return &sub, nil
}

// ListSubscriptions returns all non-unsubscribed subscriptions for an email.
func (s *SubscriptionStore) ListSubscriptions(email string) ([]Subscription, error) {
	var subs []Subscription
	if err := s.db.Where("email = ? AND status != ?", email, StatusUnsubscribed).Find(&subs).Error; err != nil {
		return nil, err
	}
	return subs, nil
}

// FindReposByIDs returns the repositories matching the given IDs.
func (s *SubscriptionStore) FindReposByIDs(ids []uint) ([]Repository, error) {
	var repos []Repository
	if err := s.db.Where("id IN ?", ids).Find(&repos).Error; err != nil {
		return nil, err
	}
	return repos, nil
}
