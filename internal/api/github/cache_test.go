//go:build unit

package github

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type fakeCache struct {
	data map[string][]byte
	ttls map[string]time.Duration
}

func newFakeCache() *fakeCache {
	return &fakeCache{
		data: make(map[string][]byte),
		ttls: make(map[string]time.Duration),
	}
}

func (c *fakeCache) Get(ctx context.Context, key string) ([]byte, error) {
	val, ok := c.data[key]
	if !ok {
		return nil, errors.New("not found")
	}
	return val, nil
}

func (c *fakeCache) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	c.data[key] = value
	c.ttls[key] = ttl
	return nil
}

type mockCacheTransport struct {
	roundTripFunc func(*http.Request) (*http.Response, error)
}

func (m *mockCacheTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if m.roundTripFunc != nil {
		return m.roundTripFunc(req)
	}
	return nil, nil
}

func TestCacheTransport_RoundTrip_CacheHit(t *testing.T) {
	fc := newFakeCache()
	endpoint := "https://api.github.com/foo"
	cacheKey := getCacheKey(endpoint)

	cachedResp := cachedHTTPResponse{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       []byte(`{"foo":"bar"}`),
	}
	b, err := json.Marshal(cachedResp)
	if err != nil {
		t.Fatalf("failed to marshal cache data: %v", err)
	}
	err = fc.Set(context.Background(), cacheKey, b, time.Minute)
	if err != nil {
		t.Fatalf("failed to set cache data: %v", err)
	}

	failTransport := &mockCacheTransport{
		roundTripFunc: func(req *http.Request) (*http.Response, error) {
			t.Fatal("expected cache hit, but transport was called")
			return nil, nil
		},
	}

	transport := NewCacheTransport(failTransport, fc, time.Minute, time.Minute)

	req := httptest.NewRequest(http.MethodGet, endpoint, nil)
	resp, err := transport.RoundTrip(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read response body: %v", err)
	}
	if string(body) != `{"foo":"bar"}` {
		t.Errorf("expected body `{\"foo\":\"bar\"}`, got %q", string(body))
	}
}

func TestCacheTransport_RoundTrip_CacheMiss(t *testing.T) {
	fc := newFakeCache()
	endpoint := "https://api.github.com/foo"

	called := false
	transport := NewCacheTransport(
		&mockCacheTransport{
			roundTripFunc: func(req *http.Request) (*http.Response, error) {
				called = true
				return fakeResponse(http.StatusOK, `{"foo":"bar"}`, map[string]string{"Content-Type": "application/json"}), nil
			},
		},
		fc,
		time.Minute,
		time.Minute,
	)

	req := httptest.NewRequest(http.MethodGet, endpoint, nil)
	resp, err := transport.RoundTrip(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()

	if !called {
		t.Fatal("expected transport to be called on cache miss")
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read response body: %v", err)
	}
	if string(body) != `{"foo":"bar"}` {
		t.Errorf("expected body `{\"foo\":\"bar\"}`, got %q", string(body))
	}

	// Verify it was cached
	cacheKey := getCacheKey(endpoint)
	cachedBytes, err := fc.Get(context.Background(), cacheKey)
	if err != nil {
		t.Fatalf("expected response to be cached, got error: %v", err)
	}

	var cached cachedHTTPResponse
	if err := json.Unmarshal(cachedBytes, &cached); err != nil {
		t.Fatalf("failed to unmarshal cached response: %v", err)
	}
	if string(cached.Body) != `{"foo":"bar"}` {
		t.Errorf("expected cached body `{\"foo\":\"bar\"}`, got %q", string(cached.Body))
	}
	if fc.ttls[cacheKey] != time.Minute {
		t.Errorf("expected TTL to be 1m, got %v", fc.ttls[cacheKey])
	}
}

func TestCacheTransport_RoundTrip_ErrorTTL(t *testing.T) {
	fc := newFakeCache()
	endpoint := "https://api.github.com/foo"

	transport := NewCacheTransport(
		&mockCacheTransport{
			roundTripFunc: func(req *http.Request) (*http.Response, error) {
				return fakeResponse(http.StatusNotFound, `{"message":"Not Found"}`, nil), nil
			},
		},
		fc,
		time.Minute,
		5*time.Minute,
	)

	req := httptest.NewRequest(http.MethodGet, endpoint, nil)
	resp, err := transport.RoundTrip(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()

	cacheKey := getCacheKey(endpoint)
	if fc.ttls[cacheKey] != 5*time.Minute {
		t.Errorf("expected error TTL to be 5m, got %v", fc.ttls[cacheKey])
	}
}

func TestCacheTransport_RoundTrip_InvalidCachedJSON(t *testing.T) {
	fc := newFakeCache()
	endpoint := "https://api.github.com/foo"
	cacheKey := getCacheKey(endpoint)

	// Set invalid JSON in the cache
	err := fc.Set(context.Background(), cacheKey, []byte("invalid json"), time.Minute)
	if err != nil {
		t.Fatalf("failed to set invalid cache data: %v", err)
	}

	called := false
	transport := NewCacheTransport(
		&mockCacheTransport{
			roundTripFunc: func(req *http.Request) (*http.Response, error) {
				called = true
				return fakeResponse(http.StatusOK, `{"foo":"bar"}`, nil), nil
			},
		},
		fc,
		time.Minute,
		time.Minute,
	)

	req := httptest.NewRequest(http.MethodGet, endpoint, nil)
	resp, err := transport.RoundTrip(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()

	if !called {
		t.Fatal("expected transport to be called due to invalid cache JSON")
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read response body: %v", err)
	}
	if string(body) != `{"foo":"bar"}` {
		t.Errorf("expected body `{\"foo\":\"bar\"}`, got %q", string(body))
	}
}
