package scanner

import (
	"context"
	"math"
	"time"

	"go.uber.org/zap"
	"golang.org/x/time/rate"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/api/github"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/logger"
)

// RateLimitProvider gives a method to get current rate limits regardless of internal implementation.
type RateLimitProvider interface {
	// GetRateLimits returns the current API rate limits.
	GetRateLimits() github.RateLimits
}

// QuotaManager abstracts GitHub API rate limits and throttling logic.
type QuotaManager interface {
	// Refresh calculates and updates the allowed requests per second based on current rate limits.
	Refresh() (rps float64)
	// Freeze sets the allowed requests per second to zero, halting all requests.
	Freeze()
	// Wait blocks until a request is allowed to proceed or the context is canceled.
	Wait(ctx context.Context) error
	// Limit returns the current allowed requests per second.
	Limit() rate.Limit
	// SetLimit updates the allowed requests per second. Useful for bypassing limits in tests.
	SetLimit(limit rate.Limit)
}

type dynamicQuotaManager struct {
	rlp          RateLimitProvider
	limiter      *rate.Limiter
	safetyBuffer float64
}

// NewQuotaManager returns a QuotaManager for tracking GitHub rate limits.
func NewQuotaManager(rlp RateLimitProvider, safetyBuffer float64) QuotaManager {
	return &dynamicQuotaManager{
		rlp:          rlp,
		limiter:      rate.NewLimiter(rate.Limit(1), 1),
		safetyBuffer: safetyBuffer,
	}
}

// Refresh recalculates the safe Requests Per Second (RPS) based on the current GitHub API rate limits.
func (q *dynamicQuotaManager) Refresh() float64 {
	var limits github.RateLimits
	if q.rlp != nil {
		limits = q.rlp.GetRateLimits()
	}
	now := time.Now()

	logger.Log.Debug("checking rate limits...")

	if now.Before(limits.RetryAfter) {
		logger.Log.Warn("secondary rate limits hit, hibernating...")
		q.limiter.SetLimit(0)
		return 0
	}

	if !limits.IsValid() {
		limits = github.GetUnauthenticatedRateLimits()
	}

	timeUntilReset := time.Until(limits.ResetAt).Seconds()
	if timeUntilReset <= 0 {
		timeUntilReset = 3600
	}

	reserved := float64(limits.Limit) * q.safetyBuffer
	usable := float64(limits.Remaining) - reserved

	var rps float64
	if usable <= 0 {
		rps = 0
		logger.Log.Warn("primary rate limit low, hibernating...", zap.Int64("remaining", limits.Remaining), zap.Int64("limit", limits.Limit))
	} else {
		rps = usable / timeUntilReset
		// don't exceed 10 RPS to avoid GitHub Secondary Limits
		rps = math.Min(rps, 10.0)
		// each repo requires up to 2 API requests (repo status + latest release)
		rps /= 2.0
	}

	q.limiter.SetLimit(rate.Limit(rps))
	return rps
}

// Freeze immediately halts all API requests by setting the rate limit to 0.
func (q *dynamicQuotaManager) Freeze() {
	q.limiter.SetLimit(0)
}

// Wait blocks the caller until the rate limiter permits an event to happen.
func (q *dynamicQuotaManager) Wait(ctx context.Context) error {
	return q.limiter.Wait(ctx)
}

// Limit returns the current maximum events per second the limiter allows.
func (q *dynamicQuotaManager) Limit() rate.Limit {
	return q.limiter.Limit()
}

// SetLimit overrides the current rate limit.
func (q *dynamicQuotaManager) SetLimit(limit rate.Limit) {
	q.limiter.SetLimit(limit)
}
