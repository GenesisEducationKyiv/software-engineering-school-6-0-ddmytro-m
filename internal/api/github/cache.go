package github

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"

	"go.uber.org/zap"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/logger"
)

// Cache defines the interface for the caching mechanism used by the GitHub client.
type Cache interface {
	Get(ctx context.Context, key string) ([]byte, error)
	Set(ctx context.Context, key string, value []byte, ttl time.Duration) error
}

// cachedHTTPResponse represents the raw HTTP data we store in the cache.
type cachedHTTPResponse struct {
	StatusCode int         `json:"status_code"`
	Header     http.Header `json:"header"`
	Body       []byte      `json:"body"`
}

// CacheTransport handles checking a cache before making a network request.
type CacheTransport struct {
	Transport http.RoundTripper
	Cache     Cache
	TTL       time.Duration
	ErrorTTL  time.Duration
}

// NewCacheTransport creates a new CacheTransport with the provided configuration.
func NewCacheTransport(transport http.RoundTripper, cache Cache, ttl, errorTTL time.Duration) *CacheTransport {
	return &CacheTransport{
		Transport: transport,
		Cache:     cache,
		TTL:       ttl,
		ErrorTTL:  errorTTL,
	}
}

func getCacheKey(endpoint string) string {
	return "github_cache:" + endpoint
}

// RoundTrip intercepts HTTP requests to return cached responses or execute and cache new ones.
func (t *CacheTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx := req.Context()
	cacheKey := getCacheKey(req.URL.String())

	if t.Cache != nil {
		if cachedBytes, err := t.Cache.Get(ctx, cacheKey); err == nil {
			var cached cachedHTTPResponse
			if err := json.Unmarshal(cachedBytes, &cached); err == nil {
				// Reconstruct and return the cached *http.Response
				return &http.Response{
					StatusCode: cached.StatusCode,
					Header:     cached.Header,
					Body:       io.NopCloser(bytes.NewReader(cached.Body)),
					Request:    req,
				}, nil
			}
		}
	}

	// Fallback to default transport if miss
	transport := t.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}

	resp, err := transport.RoundTrip(req)
	if err != nil {
		return nil, err
	}

	// Cache the response
	if t.Cache == nil || req.Method != http.MethodGet {
		return resp, nil
	}

	// Read the body entirely so we can cache it
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	// Restore the body for the original caller since we drained it
	resp.Body = io.NopCloser(bytes.NewReader(bodyBytes))

	cached := cachedHTTPResponse{
		StatusCode: resp.StatusCode,
		Header:     resp.Header,
		Body:       bodyBytes,
	}

	ttl := t.TTL
	if resp.StatusCode >= 400 {
		ttl = t.ErrorTTL
	}

	// A non-positive TTL means "don't cache". Passing 0 to Redis would persist
	// the entry forever, serving stale responses indefinitely.
	if ttl <= 0 {
		return resp, nil
	}

	if b, err := json.Marshal(cached); err == nil {
		if err = t.Cache.Set(ctx, cacheKey, b, ttl); err != nil {
			logger.Log.Error("cache transport: failed to set cache", zap.String("key", cacheKey), zap.Error(err))
		}
	}

	return resp, nil
}
