package scanner

import (
	"context"
	"log"
	"sync"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/db"
)

// WorkerPool defines the contract for processing queued repositories.
type WorkerPool interface {
	Start(ctx context.Context, wg *sync.WaitGroup, queue <-chan db.Repository)
}

type domainWorkerPool struct {
	processor   RepoProcessor
	quota       QuotaManager
	workerCount int
}

// NewWorkerPool creates a new WorkerPool instance.
func NewWorkerPool(processor RepoProcessor, quota QuotaManager, workerCount int) WorkerPool {
	return &domainWorkerPool{
		processor:   processor,
		quota:       quota,
		workerCount: workerCount,
	}
}

func (w *domainWorkerPool) Start(ctx context.Context, wg *sync.WaitGroup, queue <-chan db.Repository) {
	log.Printf("starting %d workers...", w.workerCount)
	for i := 0; i < w.workerCount; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			w.worker(ctx, id, queue)
		}(i)
	}
}

func (w *domainWorkerPool) worker(ctx context.Context, id int, queue <-chan db.Repository) {
	for {
		select {
		case <-ctx.Done():
			log.Printf("worker %d shutting down", id)
			return
		case r, ok := <-queue:
			if !ok {
				log.Printf("worker %d shutting down", id)
				return
			}

			if err := w.quota.Wait(ctx); err != nil {
				log.Printf("worker %d: limiter wait error: %v", id, err)
				return
			}
			log.Printf("worker %d: processing %s/%s", id, r.Owner, r.Name)
			w.processor.ProcessRepo(ctx, &r)
		}
	}
}
