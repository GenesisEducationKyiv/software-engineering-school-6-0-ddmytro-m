// Package scanner provides a worker that periodically checks repositories for updates.
package scanner

import (
	"context"
	"log"
	"sync"
	"time"

	"gorm.io/gorm"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/api/github"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/config"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/db"
)

// Scanner periodically checks repositories for updates.
type Scanner struct {
	store     RepositoryStore
	processor RepoProcessor
	producer  RepoProducer
	quota     QuotaManager

	repoQueue   chan db.Repository
	workerCount int

	producerInterval time.Duration
	minCheckInterval time.Duration
}

// NewScanner creates a new Scanner instance.
func NewScanner(orm *gorm.DB, ghClient *github.Client, notifier Notifier, rlp RateLimitProvider, config *config.ScannerConfig) *Scanner {
	store := NewRepositoryStore(orm)
	quota := NewQuotaManager(rlp, config.SafetyBuffer)
	processor := NewRepoProcessor(store, ghClient, notifier, quota)
	producer := NewRepoProducer(store, quota, config.ProducerInterval, config.MinInterval)

	return &Scanner{
		store:     store,
		processor: processor,
		producer:  producer,
		quota:     quota,

		repoQueue: make(chan db.Repository, config.QueueSize),

		producerInterval: config.ProducerInterval,
		workerCount:      config.Workers,
		minCheckInterval: config.MinInterval,
	}
}

// Start begins the scanning process, starting workers and the producer loop.
func (s *Scanner) Start(ctx context.Context) {
	s.recover()

	var wg sync.WaitGroup

	for i := range s.workerCount {
		wg.Go(func() {
			defer wg.Done()
			s.worker(ctx, i)
		})
	}

	ticker := time.NewTicker(s.producerInterval)
	defer ticker.Stop()

	log.Printf("scanner online: %d workers, min interval %v", s.workerCount, s.minCheckInterval)
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
	log.Println("Recovering stuck repositories...")

	err := s.store.RecoverStuckRepos()
	if err != nil {
		log.Printf("Recovery error: %v", err)
	}
}

func (s *Scanner) worker(ctx context.Context, id int) {
	for {
		select {
		case <-ctx.Done():
			log.Printf("worker %d shutting down", id)
			return
		case r, ok := <-s.repoQueue:
			if !ok {
				log.Printf("worker %d shutting down", id)
				return
			}

			if err := s.quota.Wait(ctx); err != nil {
				log.Printf("worker %d: limiter wait error: %v", id, err)
				return
			}
			log.Printf("worker %d: processing %s/%s", id, r.Owner, r.Name)
			s.processor.ProcessRepo(ctx, &r)
		}
	}
}
