package scanner

import (
	"context"
	"log"
	"math"
	"time"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/db"
)

// RepoProducer defines the contract for producing repositories to scan.
type RepoProducer interface {
	Produce(ctx context.Context, out chan<- db.Repository)
}

type domainRepoProducer struct {
	store            RepositoryStore
	quota            QuotaManager
	producerInterval time.Duration
	minCheckInterval time.Duration
}

// NewRepoProducer creates a new RepoProducer.
func NewRepoProducer(store RepositoryStore, quota QuotaManager, producerInterval, minCheckInterval time.Duration) RepoProducer {
	return &domainRepoProducer{
		store:            store,
		quota:            quota,
		producerInterval: producerInterval,
		minCheckInterval: minCheckInterval,
	}
}

func (p *domainRepoProducer) Produce(ctx context.Context, out chan<- db.Repository) {
	rps := p.quota.Refresh()
	if rps == 0 {
		return
	}

	batchSize := int(math.Ceil(rps * p.producerInterval.Seconds()))
	batchSize = min(batchSize, cap(out)-len(out)) // cap(out) eliminates the need for queueSize tracking

	if batchSize <= 0 {
		return
	}

	repos, err := p.store.ClaimIdle(batchSize, p.minCheckInterval)
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
		case out <- r:
		case <-ctx.Done():
			return
		}
	}
}
