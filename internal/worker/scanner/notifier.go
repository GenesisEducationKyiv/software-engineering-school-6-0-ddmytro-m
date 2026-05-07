package scanner

import (
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/api/github"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/db"
)

// Notifier defines the interface for sending release and repository status notifications.
type Notifier interface {
	SendNewRelease(subscriber *db.Subscription, repo *db.Repository, release *github.LatestRelease) error
	SendRepoMoved(subcriber *db.Subscription, repo *db.Repository) error
}
