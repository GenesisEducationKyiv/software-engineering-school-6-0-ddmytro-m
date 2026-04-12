package scanner

import (
	"github.com/ddmytro-m/github-scanner/internal/api/github"
	"github.com/ddmytro-m/github-scanner/internal/infra/db"
)

type Notifier interface {
	SendNewRelease(subscriber *db.Subscription, repo *db.Repository, release *github.LatestRelease) error
	SendRepoMoved(subcriber *db.Subscription, repo *db.Repository) error
}
