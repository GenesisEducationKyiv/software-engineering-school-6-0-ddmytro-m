package orchestrator

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/logger"
)

// Reaper periodically compensates onboarding sagas that have sat in
// awaiting_delivery too long - the saga-start event or the mailer's result
// was lost, so no result event will ever arrive to settle them.
type Reaper struct {
	store        Store
	pollInterval time.Duration
	staleAfter   time.Duration
}

// NewReaper creates a Reaper. pollInterval is how often it sweeps; staleAfter
// is how long a saga may sit in awaiting_delivery before being compensated.
func NewReaper(store Store, pollInterval, staleAfter time.Duration) *Reaper {
	return &Reaper{store: store, pollInterval: pollInterval, staleAfter: staleAfter}
}

// Run sweeps immediately, then on every tick, until ctx is cancelled.
func (r *Reaper) Run(ctx context.Context) {
	r.sweep(ctx)

	ticker := time.NewTicker(r.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.sweep(ctx)
		}
	}
}

func (r *Reaper) sweep(_ context.Context) {
	tokens, err := r.store.StaleAwaitingTokens(r.staleAfter)
	if err != nil {
		logger.Log.Error("saga reaper: query stale sagas failed", zap.Error(err))
		return
	}

	for _, token := range tokens {
		if err := r.store.Compensate(token); err != nil {
			logger.Log.Error("saga reaper: compensate failed", zap.String("token", token), zap.Error(err))
			continue
		}
		logger.Log.Warn("saga reaper: compensated stale saga", zap.String("token", token), zap.Duration("stale_after", r.staleAfter))
	}
}
