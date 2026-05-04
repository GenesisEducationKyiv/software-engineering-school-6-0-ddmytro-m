package scanner

import (
	"github.com/ddmytro-m/github-scanner/internal/api/github"
	"github.com/ddmytro-m/github-scanner/internal/infra/db"
)

// Notifier defines the interface for sending release and repository status notifications.
type Notifier interface {
	SendNewRelease(subscriber *db.Subscription, repo *db.Repository, release *github.LatestRelease) error
	SendRepoMoved(subcriber *db.Subscription, repo *db.Repository) error
}
