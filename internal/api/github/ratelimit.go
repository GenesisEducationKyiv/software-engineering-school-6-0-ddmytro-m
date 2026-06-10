package github

import (
	"net/http"
	"strconv"
	"sync"
	"time"
)

// RateLimits holds the current state of GitHub API usage limits and reset times.
type RateLimits struct {
	Limit      int64
	Remaining  int64
	ResetAt    time.Time
	RetryAfter time.Time
}

// IsValid checks if the rate limits are correctly initialized.
func (rl *RateLimits) IsValid() bool {
	return rl.Limit != -1 && rl.Remaining != -1 && !rl.ResetAt.IsZero()
}

// RateLimitTransport intercepts responses to track GitHub rate limits.
type RateLimitTransport struct {
	Transport http.RoundTripper

	mu         sync.RWMutex
	lastLimits RateLimits
}

// NewRateLimitTransport creates a new RateLimitTransport with the provided transport.
func NewRateLimitTransport(transport http.RoundTripper, baseLimits RateLimits) *RateLimitTransport {
	return &RateLimitTransport{
		Transport:  transport,
		lastLimits: baseLimits,
	}
}

// RoundTrip executes the HTTP request and extracts rate limit headers from the response.
func (t *RateLimitTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	transport := t.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}

	res, err := transport.RoundTrip(req)
	if err != nil {
		return nil, err
	}

	limits := extractRateLimits(res.Header)

	t.mu.Lock()
	if limits.IsValid() {
		t.lastLimits.Limit = limits.Limit
		t.lastLimits.Remaining = limits.Remaining
		t.lastLimits.ResetAt = limits.ResetAt
	}

	if !limits.RetryAfter.IsZero() && limits.RetryAfter.Sub(t.lastLimits.RetryAfter) > time.Second {
		// If general limits are missing but a Retry-After is present (e.g., secondary limits)
		t.lastLimits.RetryAfter = limits.RetryAfter
	}
	t.mu.Unlock()

	return res, nil
}

// GetRateLimits allows the application to safely read the most recently observed limits.
func (t *RateLimitTransport) GetRateLimits() RateLimits {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.lastLimits
}

// Helper functions
func extractRateLimits(h http.Header) RateLimits {
	return RateLimits{
		Limit:      getInt64Header(h, "X-RateLimit-Limit"),
		Remaining:  getInt64Header(h, "X-RateLimit-Remaining"),
		ResetAt:    getResetTime(h),
		RetryAfter: getRetryAfterTime(h, time.Now()),
	}
}

func getInt64Header(h http.Header, key string) int64 {
	val := h.Get(key)
	if val == "" {
		return -1
	}

	parsed, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return -1
	}

	return parsed
}

func getResetTime(h http.Header) time.Time {
	val := h.Get("X-RateLimit-Reset")
	if val == "" {
		return time.Time{}
	}

	resetUnix, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return time.Time{}
	}

	return time.Unix(resetUnix, 0)
}

func getRetryAfterTime(h http.Header, now time.Time) time.Time {
	val := h.Get("Retry-After")
	if val == "" {
		return time.Time{}
	}

	// time in seconds
	if seconds, err := strconv.ParseInt(val, 10, 64); err == nil {
		return now.Add(time.Duration(seconds) * time.Second)
	}

	// time as http date
	if t, err := http.ParseTime(val); err == nil {
		return t
	}

	return time.Time{}
}

// GetBaseRateLimits returns the default rate limits based on whether a token is configured.
func GetBaseRateLimits(token string) RateLimits {
	if token != "" {
		return GetAuthenticatedRateLimits()
	}
	return GetUnauthenticatedRateLimits()
}

// GetUnauthenticatedRateLimits returns the default rate limits for unauthenticated requests.
func GetUnauthenticatedRateLimits() RateLimits {
	return RateLimits{Limit: 60, Remaining: 60, ResetAt: time.Now().Add(1 * time.Hour)}
}

// GetAuthenticatedRateLimits returns the default rate limits for authenticated requests.
func GetAuthenticatedRateLimits() RateLimits {
	return RateLimits{Limit: 5000, Remaining: 5000, ResetAt: time.Now().Add(1 * time.Hour)}
}
