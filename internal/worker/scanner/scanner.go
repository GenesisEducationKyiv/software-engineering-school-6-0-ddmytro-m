// Package scanner provides a worker that periodically checks repositories for updates.
package scanner

import (
	"context"
	"log"
	"math"
	"sync"
	"time"

	"gorm.io/gorm"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/api/github"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/config"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/db"
)

// Scanner periodically checks repositories for updates.
type Scanner struct {
	store RepositoryStore
	gh    *github.Client

	notifier Notifier
	quota    QuotaManager

	repoQueue chan db.Repository
	queueSize int

	workerCount int

	producerInterval time.Duration
	minCheckInterval time.Duration
	safetyBuffer     float64 // e.g. 20%
}

// NewScanner creates a new Scanner instance.
func NewScanner(orm *gorm.DB, ghClient *github.Client, notifier Notifier, rlp RateLimitProvider, config *config.ScannerConfig) *Scanner {
	return &Scanner{
		store:    NewRepositoryStore(orm),
		gh:       ghClient,
		notifier: notifier,

		quota: NewQuotaManager(rlp, config.SafetyBuffer),

		repoQueue: make(chan db.Repository, config.QueueSize),
		queueSize: config.QueueSize,

		producerInterval: config.ProducerInterval,
		workerCount:      config.Workers,
		minCheckInterval: config.MinInterval,
		safetyBuffer:     config.SafetyBuffer,
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
	s.produce(ctx)

	for {
		select {
		case <-ctx.Done():
			close(s.repoQueue)
			wg.Wait()
			return
		case <-ticker.C:
			s.produce(ctx)
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

func (s *Scanner) produce(ctx context.Context) {
	rps := s.quota.Refresh()
	if rps == 0 {
		return
	}

	batchSize := int(math.Ceil(rps * s.producerInterval.Seconds()))
	batchSize = min(batchSize, s.queueSize-len(s.repoQueue))

	if batchSize <= 0 {
		return
	}

	repos, err := s.store.ClaimIdle(batchSize, s.minCheckInterval)

	if err != nil {
		log.Printf("producer db error: %v", err)
		return
	}
	if len(repos) == 0 {
		log.Print("no repositories to scan")
		return
	}

	log.Printf("found %d repositories to scan", len(repos))

	for _, r := range repos {
		select {
		case s.repoQueue <- r:
		case <-ctx.Done():
			return
		}
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
			s.processRepo(ctx, &r)
		}
	}
}

func (s *Scanner) processRepo(ctx context.Context, repo *db.Repository) {
	defer func() {
		if err := s.store.UpdateScanStatus(repo); err != nil {
			log.Printf("error updating scan status for %s/%s: %v", repo.Owner, repo.Name, err)
		}
	}()

	repoResp := s.gh.GetRepository(ctx, repo.Owner, repo.Name, repo.ETag)

	switch repoResp.StatusCode {
	case 200:
		if repoResp.Data.ID != repo.GitHubID {
			log.Printf("repo ID mismatch for %s/%s: stored %d, got %d — skipping", repo.Owner, repo.Name, repo.GitHubID, repoResp.Data.ID)
			s.handleRepoMoved(repo)
			return
		}
		repo.ETag = repoResp.ETag

	case 304:
		// identity confirmed via ETag, proceed

	case 404:
		log.Printf("repo %s/%s no longer exists — skipping", repo.Owner, repo.Name)
		return

	case 403, 429:
		s.quota.Freeze()
		log.Printf("critical limit hit on repo check (%d). limiter frozen.", repoResp.StatusCode)
		return

	default:
		if repoResp.Error != nil {
			log.Printf("error checking repo %s/%s: %v", repo.Owner, repo.Name, repoResp.Error)
		}
		return
	}

	releaseResp := s.gh.GetLatestRelease(ctx, repo.Owner, repo.Name, repo.LastRelease.ETag)

	switch releaseResp.StatusCode {
	case 200:
		if releaseResp.Data.TagName != repo.LastRelease.TagName {
			log.Printf("new release for %s/%s: %s", repo.Owner, repo.Name, releaseResp.Data.TagName)
			s.handleNewRelease(repo, &releaseResp.Data)
			repo.LastRelease.TagName = releaseResp.Data.TagName
			repo.LastRelease.GitHubID = releaseResp.Data.ID
		}
		repo.LastRelease.ETag = releaseResp.ETag

	case 304:
		// no change

	case 404:
		// repo have no latest release

	case 403, 429:
		s.quota.Freeze()
		log.Printf("critical limit hit (%d). limiter frozen.", releaseResp.StatusCode)

	default:
		if releaseResp.Error != nil {
			log.Printf("error while getting latest release: %s", releaseResp.Error.Error())
		}
	}
}

func (s *Scanner) handleNewRelease(repo *db.Repository, latestRelease *github.LatestRelease) {
	subs, err := s.store.GetActiveSubscriptions(repo.ID)
	if err != nil {
		log.Printf("error finding active subscriptions for %s/%s: %v", repo.Owner, repo.Name, err)
		return
	}

	for _, sub := range subs {
		if err := s.notifier.SendNewRelease(&sub, repo, latestRelease); err != nil {
			log.Printf("failed to notify %s for %s/%s: %v", sub.Email, repo.Owner, repo.Name, err)
		}
	}
}

func (s *Scanner) handleRepoMoved(repo *db.Repository) {
	subs, err := s.store.GetActiveSubscriptions(repo.ID)
	if err != nil {
		log.Printf("error finding active subscriptions for %s/%s: %v", repo.Owner, repo.Name, err)
		return
	}

	for _, sub := range subs {
		if err := s.notifier.SendRepoMoved(&sub, repo); err != nil {
			log.Printf("failed to notify %s for %s/%s: %v", sub.Email, repo.Owner, repo.Name, err)
		}
	}

	if err := s.store.MarkMovedAndUnsubscribe(repo); err != nil {
		log.Printf("failed to handle db updates for moved repo %s/%s: %v", repo.Owner, repo.Name, err)
	}
}
