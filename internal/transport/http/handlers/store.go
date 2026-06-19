package handlers

import (
	"context"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/api/github"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/db"
)

// SubscriptionRepository defines data access methods for the subscription handler.
// It is satisfied by *db.SubscriptionStore; the handler declares only what it needs.
type SubscriptionRepository interface {
	FindRepoByPath(owner, name string) (*db.Repository, error)
	FindRepoByGitHubID(githubID int64) (*db.Repository, error)
	SaveRepo(repo *db.Repository) error
	CreateRepo(repo *db.Repository) error
	FindSubscription(email string, repoID uint) (*db.Subscription, error)
	IsConfirmTokenTaken(token string) (bool, error)
	SaveSubscription(sub *db.Subscription) error
	CreateSubscription(sub *db.Subscription) error
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
