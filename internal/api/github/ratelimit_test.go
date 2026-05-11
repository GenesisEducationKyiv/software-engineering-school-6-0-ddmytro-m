//go:build unit

package github

import (
	"net/http"
	"strconv"
	"sync"
	"testing"
	"time"
)

// mockTransport lets us intercept the RoundTrip to simulate GitHub responses
type mockTransport struct {
	roundTripFunc func(req *http.Request) (*http.Response, error)
}

func (m *mockTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return m.roundTripFunc(req)
}

func TestRateLimitTransport_StoresValidLimits(t *testing.T) {
	reset := time.Now().Add(time.Hour).Truncate(time.Second)

	transport := &mockTransport{
		roundTripFunc: func(req *http.Request) (*http.Response, error) {
			res := &http.Response{Header: make(http.Header)}
			res.Header.Set("X-RateLimit-Limit", "5000")
			res.Header.Set("X-RateLimit-Remaining", "4000")
			res.Header.Set("X-RateLimit-Reset", strconv.FormatInt(reset.Unix(), 10))
			return res, nil
		},
	}

	rl := NewRateLimitTransport(transport, RateLimits{})
	req, err := http.NewRequest("GET", "/", nil)
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}
	if _, err := rl.RoundTrip(req); err != nil {
		t.Fatalf("RoundTrip failed: %v", err)
	}

	got := rl.GetRateLimits()
	if got.Limit != 5000 || got.Remaining != 4000 {
		t.Errorf("unexpected limits after update: %+v", got)
	}
	if !got.ResetAt.Equal(reset) {
		t.Errorf("ResetAt = %v, want %v", got.ResetAt, reset)
	}
}

func TestRateLimitTransport_AllMissingIsIgnored(t *testing.T) {
	reset := time.Now().Add(time.Hour).Truncate(time.Second)
	baseLimits := RateLimits{Limit: 5000, Remaining: 4000, ResetAt: reset}

	transport := &mockTransport{
		roundTripFunc: func(req *http.Request) (*http.Response, error) {
			// Response without any rate limit headers
			return &http.Response{Header: make(http.Header)}, nil
		},
	}

	rl := NewRateLimitTransport(transport, baseLimits)
	req, err := http.NewRequest("GET", "/", nil)
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}
	if _, err := rl.RoundTrip(req); err != nil {
		t.Fatalf("RoundTrip failed: %v", err)
	}

	if got := rl.GetRateLimits(); got.Limit != 5000 {
		t.Errorf("limits should not be overwritten by all-missing update; got Limit=%d", got.Limit)
	}
}

func TestRateLimitTransport_RetryAfterIsStoredWhenLimitsMissing(t *testing.T) {
	baseLimits := RateLimits{Limit: -1, Remaining: -1}

	transport := &mockTransport{
		roundTripFunc: func(req *http.Request) (*http.Response, error) {
			res := &http.Response{Header: make(http.Header)}
			res.Header.Set("Retry-After", "60") // Wait 60 seconds
			return res, nil
		},
	}

	rl := NewRateLimitTransport(transport, baseLimits)
	req, err := http.NewRequest("GET", "/", nil)
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}
	if _, err := rl.RoundTrip(req); err != nil {
		t.Fatalf("RoundTrip failed: %v", err)
	}

	got := rl.GetRateLimits()

	expected := time.Now().Add(60 * time.Second)
	diff := got.RetryAfter.Sub(expected)

	// Allow a 1-second margin of error for test execution time
	if diff < -time.Second || diff > time.Second {
		t.Errorf("Expected RetryAfter near %v, got %v", expected, got.RetryAfter)
	}
}

func TestUpdateInternalLimits_RetryAfterOnlyMovesForward(t *testing.T) {
	transport := &mockTransport{
		roundTripFunc: func(req *http.Request) (*http.Response, error) {
			res := &http.Response{Header: make(http.Header)}
			res.Header.Set("Retry-After", req.Header.Get("X-Test-Retry"))
			return res, nil
		},
	}
	rl := NewRateLimitTransport(transport, RateLimits{})

	later := time.Now().Add(90 * time.Second)
	_ = time.Now().Add(30 * time.Second) // earlier

	// First request sets a later Retry-After
	req1, err := http.NewRequest("GET", "/", nil)
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}
	req1.Header.Set("X-Test-Retry", "90")
	if _, err := rl.RoundTrip(req1); err != nil {
		t.Fatalf("RoundTrip 1 failed: %v", err)
	}

	// Second request tries to set an earlier Retry-After
	req2, err := http.NewRequest("GET", "/", nil)
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}
	req2.Header.Set("X-Test-Retry", "30")
	rl.RoundTrip(req2)

	if got := rl.GetRateLimits(); got.RetryAfter.Sub(later) > time.Second {
		t.Errorf("RetryAfter should stay at later value; got %v, want %v", got.RetryAfter, later)
	}
}

func TestRateLimitTransport_ConcurrentSafety(t *testing.T) {
	transport := &mockTransport{
		roundTripFunc: func(req *http.Request) (*http.Response, error) {
			res := &http.Response{Header: make(http.Header)}
			res.Header.Set("X-RateLimit-Limit", "60")
			res.Header.Set("X-RateLimit-Remaining", "59")
			res.Header.Set("X-RateLimit-Reset", "1372700873")
			return res, nil
		},
	}

	rl := NewRateLimitTransport(transport, RateLimits{})

	var wg sync.WaitGroup
	routines := 50

	wg.Add(routines)
	for i := 0; i < routines; i++ {
		go func() {
			defer wg.Done()

			req, err := http.NewRequest("GET", "/", nil)
			if err != nil {
				return
			}

			// Simulate writing limits
			if _, err := rl.RoundTrip(req); err != nil {
				t.Errorf("RoundTrip failed: %v", err)
			}

			// Simulate reading limits concurrently
			_ = rl.GetRateLimits()
		}()
	}
	wg.Wait()

	limits := rl.GetRateLimits()
	if limits.Limit != 60 {
		t.Errorf("Expected Limit 60, got %d", limits.Limit)
	}
	if limits.Remaining != 59 {
		t.Errorf("Expected Remaining 59, got %d", limits.Remaining)
	}
	if !limits.ResetAt.Equal(time.Unix(1372700873, 0)) {
		t.Errorf("Expected ResetAt 1372700873, got %v", limits.ResetAt)
	}
}
