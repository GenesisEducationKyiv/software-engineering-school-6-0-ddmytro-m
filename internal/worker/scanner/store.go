package scanner

import (
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/db"
)

// RepositoryStore defines data access methods for the scanner.
type RepositoryStore interface {
	RecoverStuckRepos() error
	ClaimIdle(batchSize int, minInterval time.Duration) ([]db.Repository, error)
	UpdateScanStatus(repo *db.Repository) error
	GetActiveSubscriptions(repoID uint) ([]db.Subscription, error)
	MarkMovedAndUnsubscribe(repo *db.Repository) error
}

type gormStore struct {
	db *gorm.DB
}

// NewRepositoryStore creates a new GORM-backed RepositoryStore.
func NewRepositoryStore(db *gorm.DB) RepositoryStore {
	return &gormStore{db: db}
}

func (s *gormStore) RecoverStuckRepos() error {
	return s.db.Model(&db.Repository{}).
		Where("status = ?", "processing").
		Updates(map[string]any{"status": "idle"}).Error
}

func (s *gormStore) ClaimIdle(batchSize int, minInterval time.Duration) ([]db.Repository, error) {
	var ids []uint
	var repos []db.Repository

	err := s.db.Transaction(func(tx *gorm.DB) error {
		// Lock the candidate rows and skip any already locked by another
		// producer instance, so concurrent scanners never claim the same repos.
		err := tx.Clauses(clause.Locking{
			Strength: clause.LockingStrengthUpdate,
			Options:  clause.LockingOptionsSkipLocked,
		}).Model(&db.Repository{}).
			Select("id").
			Where("status = ? AND (last_scanned_at IS NULL OR last_scanned_at <= ?)", "idle", time.Now().Add(-minInterval)).
			Order("last_scanned_at ASC").
			Limit(batchSize).
			Pluck("id", &ids).Error

		if err != nil || len(ids) == 0 {
			return err
		}

		return tx.Model(&db.Repository{}).
			Where("id IN ?", ids).
			Updates(map[string]any{
				"status":          "processing",
				"last_scanned_at": time.Now(),
			}).Error
	})

	if err == nil && len(ids) > 0 {
		s.db.Find(&repos, ids)
	}
	return repos, err
}

func (s *gormStore) UpdateScanStatus(repo *db.Repository) error {
	return s.db.Model(repo).Updates(map[string]any{
		"status":                 "idle",
		"last_scanned_at":        time.Now(),
		"e_tag":                  repo.ETag,
		"last_release_github_id": repo.LastRelease.GitHubID,
		"last_release_tag_name":  repo.LastRelease.TagName,
		"last_release_e_tag":     repo.LastRelease.ETag,
	}).Error
}

func (s *gormStore) GetActiveSubscriptions(repoID uint) ([]db.Subscription, error) {
	var subs []db.Subscription
	err := s.db.Where("repository_id = ? AND status = ?", repoID, db.StatusActive).Find(&subs).Error
	return subs, err
}

func (s *gormStore) MarkMovedAndUnsubscribe(repo *db.Repository) error {
	if err := s.db.Model(&db.Subscription{}).
		Where("repository_id = ?", repo.ID).
		Update("status", db.StatusUnsubscribed).Error; err != nil {
		return err
	}
	return s.db.Delete(repo).Error
}
