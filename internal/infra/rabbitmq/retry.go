package rabbitmq

import (
	"fmt"
	"time"
)

// RetryPolicy configures tiered exponential backoff. Retry tier i (0-based)
// waits BaseTTL * Factor^i before the message returns to the main queue.
// Tiers equals the maximum number of retries before dead-lettering.
type RetryPolicy struct {
	BaseTTL time.Duration
	Factor  int
	Tiers   int
}

// NewRetryPolicy builds a RetryPolicy, clamping Factor to >= 1 and Tiers to >= 0.
func NewRetryPolicy(baseTTL time.Duration, factor, tiers int) RetryPolicy {
	if factor < 1 {
		factor = 1
	}
	if tiers < 0 {
		tiers = 0
	}
	return RetryPolicy{BaseTTL: baseTTL, Factor: factor, Tiers: tiers}
}

func (p RetryPolicy) tierTTLms(tier int) int64 {
	ms := p.BaseTTL.Milliseconds()
	for range tier {
		ms *= int64(p.Factor)
	}
	return ms
}

func retryQueueName(base string, tier int) string {
	return fmt.Sprintf("%s.%d", base, tier)
}
