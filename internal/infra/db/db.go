package db

import (
	"log"
	"sync"

	"github.com/ddmytro-m/github-scanner/internal/config"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type Release struct {
	GitHubID int64  `gorm:"column:github_id"`
	TagName  string `gorm:"column:tag_name;size:255"`
	ETag     string `gorm:"column:etag;size:255"`
}

type Repository struct {
	gorm.Model

	GitHubID int64  `gorm:"column:github_id;uniqueIndex;not null"`
	ETag     string `gorm:"size:255"`

	Owner string `gorm:"index:idx_repo_path;size:64"`
	Name  string `gorm:"index:idx_repo_path;size:128"`

	LastRelease   Release        `gorm:"embedded;embeddedPrefix:last_release_"`
	Subscriptions []Subscription `gorm:"foreignKey:RepositoryID"`
}

type SubscriptionStatus string

const (
	StatusPending      SubscriptionStatus = "pending"
	StatusActive       SubscriptionStatus = "active"
	StatusUnsubscribed SubscriptionStatus = "unsubscribed"
)

type Subscription struct {
	gorm.Model

	Email        string `gorm:"index:idx_email_repo,unique;size:255"`
	RepositoryID uint   `gorm:"index:idx_email_repo,unique"`

	Status SubscriptionStatus `gorm:"type:varchar(20);default:'pending'"`

	ConfirmToken string `gorm:"uniqueIndex;size:32"`
	ApiToken     string `gorm:"uniqueIndex;size:32"`
}

var (
	once     sync.Once
	instance *gorm.DB
)

func Get() *gorm.DB {
	once.Do(func() {
		dsn := config.Get().DBDSN
		db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
		if err != nil {
			panic("failed to connect to database: " + err.Error())
		}

		db.AutoMigrate(&Repository{}, &Subscription{})

		instance = db
	})

	return instance
}

func Close() {
	if instance != nil {
		sqlDB, err := instance.DB()
		if err != nil {
			log.Printf("error getting underlying sql.DB: %v", err)
			return
		}

		if err := sqlDB.Close(); err != nil {
			log.Printf("error closing database: %v", err)
		} else {
			log.Println("database connection closed.")
		}
	}
}
