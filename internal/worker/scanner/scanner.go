// Package scanner provides a worker that periodically checks repositories for updates.
package scanner

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/api/github"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/config"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/db"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/logger"
)

// Scanner periodically checks repositories for updates.
type Scanner struct {
	store     RepositoryStore
	processor RepoProcessor
	producer  RepoProducer
	workers   WorkerPool

	quota     QuotaManager
	repoQueue chan db.Repository

	producerInterval time.Duration
	minCheckInterval time.Duration
}

// NewScanner creates a new Scanner instance.
func NewScanner(orm *gorm.DB, ghClient *github.Client, notifier Notifier, rlp RateLimitProvider, config *config.ScannerConfig) *Scanner {
	store := NewRepositoryStore(orm)
	quota := NewQuotaManager(rlp, config.SafetyBuffer)
	processor := NewRepoProcessor(store, ghClient, notifier, quota)
	producer := NewRepoProducer(store, quota, config.ProducerInterval, config.MinInterval)
	workers := NewWorkerPool(processor, quota, config.Workers)

	return &Scanner{
		store:     store,
		processor: processor,
		producer:  producer,
		workers:   workers,
		quota:     quota,

		repoQueue: make(chan db.Repository, config.QueueSize),

		producerInterval: config.ProducerInterval,
		minCheckInterval: config.MinInterval,
	}
}

// Start begins the scanning process, starting workers and the producer loop.
func (s *Scanner) Start(ctx context.Context) {
	s.recover()

	var wg sync.WaitGroup

	s.workers.Start(ctx, &wg, s.repoQueue)

	ticker := time.NewTicker(s.producerInterval)
	defer ticker.Stop()

	logger.Log.Info("scanner online", zap.Duration("min_interval", s.minCheckInterval))
	s.producer.Produce(ctx, s.repoQueue)

	for {
		select {
		case <-ctx.Done():
			close(s.repoQueue)
			wg.Wait()
			return
		case <-ticker.C:
			s.producer.Produce(ctx, s.repoQueue)
		}
	}
}

func (s *Scanner) recover() {
	logger.Log.Info("Recovering stuck repositories...")

	err := s.store.RecoverStuckRepos()
	if err != nil {
		logger.Log.Error("Recovery error", zap.Error(err))
	}
}
