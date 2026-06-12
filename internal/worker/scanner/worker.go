package scanner

import (
	"context"
	"sync"

	"go.uber.org/zap"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/db"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/logger"
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
	logger.Log.Info("starting workers", zap.Int("count", w.workerCount))
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
			logger.Log.Info("worker shutting down", zap.Int("worker_id", id))
			return
		case r, ok := <-queue:
			if !ok {
				logger.Log.Info("worker shutting down", zap.Int("worker_id", id))
				return
			}

			if err := w.quota.Wait(ctx); err != nil {
				logger.Log.Error("worker limiter wait error", zap.Int("worker_id", id), zap.Error(err))
				return
			}
			logger.Log.Debug("processing repo", zap.Int("worker_id", id), zap.String("owner", r.Owner), zap.String("name", r.Name))
			w.processor.ProcessRepo(ctx, &r)
		}
	}
}
