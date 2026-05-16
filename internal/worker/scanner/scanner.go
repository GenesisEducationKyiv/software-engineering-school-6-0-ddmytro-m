// Package scanner provides a worker that periodically checks repositories for updates.
package scanner

import (
	"context"
	"log"
	"math"
	"sync"
	"time"

	"golang.org/x/time/rate"
	"gorm.io/gorm"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/api/github"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/config"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/db"
)

// RateLimitProvider gives a method to get current rate limits regardless of internal implementation
type RateLimitProvider interface {
	GetRateLimits() github.RateLimits
}

// Scanner periodically checks repositories for updates.
type Scanner struct {
	db *gorm.DB
	gh *github.Client

	notifier Notifier
	limiter  *rate.Limiter

	rlp RateLimitProvider

	repoQueue chan db.Repository
	queueSize int

	workerCount int

	producerInterval time.Duration
	minCheckInterval time.Duration
	safetyBuffer     float64 // e.g. 20%
}

// NewScanner creates a new Scanner instance.
func NewScanner(orm *gorm.DB, ghClient *github.Client, notifier Notifier, rlp RateLimitProvider, config *config.ScannerConfig) *Scanner {
	limiter := rate.NewLimiter(rate.Limit(1), 1)

	return &Scanner{
		db:       orm,
		gh:       ghClient,
		notifier: notifier,
		limiter:  limiter,

		rlp: rlp,

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

	err := s.db.Model(&db.Repository{}).
		Where("status = ?", "processing").
		Updates(map[string]any{
			"status": "idle",
		}).Error

	if err != nil {
		log.Printf("Recovery error: %v", err)
	}
}

func (s *Scanner) produce(ctx context.Context) {
	var limits github.RateLimits
	if s.rlp != nil {
		limits = s.rlp.GetRateLimits()
	}
	now := time.Now()

	log.Print("checking rate limits...")

	if now.Before(limits.RetryAfter) {
		log.Print("secondary rate limits hit, hibernating...")
		s.limiter.SetLimit(0)
		return
	}

	if !limits.IsValid() {
		limits = github.GetUnauthenticatedRateLimits() // scanner implies the lack of token by default
	}

	timeUntilReset := time.Until(limits.ResetAt).Seconds()
	if timeUntilReset <= 0 {
		timeUntilReset = 3600
	}

	reserved := float64(limits.Limit) * s.safetyBuffer
	usable := float64(limits.Remaining) - reserved

	var rps float64
	if usable <= 0 {
		rps = 0
		log.Printf("primary rate limit low (%d/%d). hibernating...", limits.Remaining, limits.Limit)
	} else {
		rps = usable / timeUntilReset
		// don't exceed 10 RPS to avoid GitHub Secondary Limits
		rps = math.Min(rps, 10.0)
		// each repo requires up to 2 API requests (repo status + latest release)
		rps /= 2.0
	}

	s.limiter.SetLimit(rate.Limit(rps))
	if rps == 0 {
		return
	}

	batchSize := int(math.Ceil(rps * s.producerInterval.Seconds()))
	batchSize = min(batchSize, s.queueSize-len(s.repoQueue))

	if batchSize <= 0 {
		return
	}

	var ids []uint

	err := s.db.Transaction(func(tx *gorm.DB) error {
		err := tx.Model(&db.Repository{}).
			Select("id").
			Where("status = ? AND (last_scanned_at IS NULL OR last_scanned_at <= ?)", "idle", time.Now().Add(-s.minCheckInterval)).
			Order("last_scanned_at ASC").
			Limit(batchSize).
			Pluck("id", &ids).Error

		if err != nil {
			return err
		}
		if len(ids) == 0 {
			log.Print("no repositories to scan")
			return nil
		}

		log.Printf("found %d repositories to scan", len(ids))

		return tx.Model(&db.Repository{}).
			Where("id IN ?", ids).
			Updates(map[string]any{
				"status":          "processing",
				"last_scanned_at": time.Now(), // update early to prevent re-queuing while processing
			}).Error
	})

	if err != nil {
		log.Printf("producer db error: %v", err)
		return
	}

	var repos []db.Repository
	s.db.Find(&repos, ids)

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

			if err := s.limiter.Wait(ctx); err != nil {
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
		s.db.Model(repo).Updates(map[string]any{
			"status":                 "idle",
			"last_scanned_at":        time.Now(), // accurate timestamp after processing completes
			"e_tag":                  repo.ETag,
			"last_release_github_id": repo.LastRelease.GitHubID,
			"last_release_tag_name":  repo.LastRelease.TagName,
			"last_release_e_tag":     repo.LastRelease.ETag,
		})
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
		s.limiter.SetLimit(0)
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
		s.limiter.SetLimit(0)
		log.Printf("critical limit hit (%d). limiter frozen.", releaseResp.StatusCode)

	default:
		if releaseResp.Error != nil {
			log.Printf("error while getting latest release: %s", releaseResp.Error.Error())
		}
	}
}

func (s *Scanner) handleNewRelease(repo *db.Repository, latestRelease *github.LatestRelease) {
	var subs []db.Subscription
	if err := s.db.Where("repository_id = ? AND status = ?", repo.ID, db.StatusActive).Find(&subs).Error; err != nil {
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
	var subs []db.Subscription
	if err := s.db.Where("repository_id = ? AND status = ?", repo.ID, db.StatusActive).Find(&subs).Error; err != nil {
		log.Printf("error finding active subscriptions for %s/%s: %v", repo.Owner, repo.Name, err)
		return
	}

	for _, sub := range subs {
		if err := s.notifier.SendRepoMoved(&sub, repo); err != nil {
			log.Printf("failed to notify %s for %s/%s: %v", sub.Email, repo.Owner, repo.Name, err)
		}
	}

	if err := s.db.Model(&db.Subscription{}).
		Where("repository_id = ?", repo.ID).
		Update("status", db.StatusUnsubscribed).Error; err != nil {
		log.Printf("failed to unsubscribe users for %s/%s: %v", repo.Owner, repo.Name, err)
	}

	if err := s.db.Delete(repo).Error; err != nil {
		log.Printf("failed to soft-delete stale repo %s/%s: %v", repo.Owner, repo.Name, err)
	}
}
