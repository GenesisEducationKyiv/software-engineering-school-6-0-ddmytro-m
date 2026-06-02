// Package db provides database models and connection management.
package db

import (
	"sync"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	"go.uber.org/zap"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/config"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/logger"
)

// Release represents the latest release information for a repository.
type Release struct {
	GitHubID int64  `gorm:"column:github_id"`
	TagName  string `gorm:"column:tag_name;size:255"`
	ETag     string `gorm:"size:255"`
}

// RepositoryStatus represents the current processing status of a repository.
type RepositoryStatus string

// Repository status constants.
const (
	StatusIdle       RepositoryStatus = "idle"
	StatusProcessing RepositoryStatus = "processing"
)

// Repository represents a GitHub repository to be scanned.
type Repository struct {
	gorm.Model

	GitHubID int64  `gorm:"column:github_id;uniqueIndex:idx_github_id,where:deleted_at IS NULL;not null"`
	ETag     string `gorm:"size:255"`

	Owner string `gorm:"uniqueIndex:idx_active_repo_path,where:deleted_at IS NULL;size:64"`
	Name  string `gorm:"uniqueIndex:idx_active_repo_path,where:deleted_at IS NULL;size:128"`

	LastRelease Release `gorm:"embedded;embeddedPrefix:last_release_"`

	Status        RepositoryStatus `gorm:"type:varchar(20);default:'idle';index"`
	LastScannedAt *time.Time       `gorm:"index"`

	Subscriptions []Subscription `gorm:"foreignKey:RepositoryID"`
}

// SubscriptionStatus represents the current status of a user's subscription.
type SubscriptionStatus string

// Subscription status constants.
const (
	StatusPending      SubscriptionStatus = "pending"
	StatusActive       SubscriptionStatus = "active"
	StatusUnsubscribed SubscriptionStatus = "unsubscribed"
)

// Subscription represents a user's subscription to a repository's releases.
type Subscription struct {
	gorm.Model

	Email        string `gorm:"uniqueIndex:idx_email_repo,where:deleted_at IS NULL;size:255"`
	RepositoryID uint   `gorm:"uniqueIndex:idx_email_repo,where:deleted_at IS NULL"`

	Status SubscriptionStatus `gorm:"type:varchar(20);default:'pending'"`

	ConfirmToken string `gorm:"uniqueIndex:idx_confirm_token,where:deleted_at IS NULL;size:32"`
	APIToken     string `gorm:"column:api_token;index:idx_api_token,where:deleted_at IS NULL;size:32"`
}

var (
	once     sync.Once
	instance *gorm.DB
)

// Get returns the singleton database instance, initializing it if necessary.
func Get() *gorm.DB {
	once.Do(func() {
		dsn := config.Get().DBDSN
		db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
		if err != nil {
			panic("failed to connect to database: " + err.Error())
		}

		err = db.AutoMigrate(&Repository{}, &Subscription{})
		if err != nil {
			panic("failed to migrate database: " + err.Error())
		}

		instance = db
	})

	return instance
}

// Close closes the underlying SQL database connection.
func Close() {
	if instance != nil {
		sqlDB, err := instance.DB()
		if err != nil {
			logger.Log.Error("error getting underlying sql.DB", zap.Error(err))
			return
		}

		if err := sqlDB.Close(); err != nil {
			logger.Log.Error("error closing database", zap.Error(err))
		} else {
			logger.Log.Info("database connection closed")
		}
	}
}
