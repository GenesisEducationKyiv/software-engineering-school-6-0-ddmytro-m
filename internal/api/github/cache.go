package github

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"time"
)

// Cache defines the interface for the caching mechanism used by the GitHub client.
type Cache interface {
	Get(ctx context.Context, key string) ([]byte, error)
	Set(ctx context.Context, key string, value []byte, ttl time.Duration) error
}

type cachedResponse[T any] struct {
	Data       T            `json:"Data"`
	ETag       string       `json:"ETag"`
	StatusCode int          `json:"StatusCode"`
	RateLimits RateLimits   `json:"RateLimits"`
	Err        *cachedError `json:"Err,omitempty"`
}

func toCached[T any](r Response[T]) cachedResponse[T] {
	cr := cachedResponse[T]{
		Data:       r.Data,
		ETag:       r.ETag,
		StatusCode: r.StatusCode,
		RateLimits: r.RateLimits,
	}

	if r.Error != nil {
		{
			var e *APIError
			var e1 *DecodingError
			var e2 *UnexpectedStatusError
			var e3 *NetworkError
			switch {
			case errors.As(r.Error, &e):
				cr.Err = &cachedError{Type: "api", Message: e.Message, Code: e.StatusCode, DocumentationURL: e.DocumentationURL}
			case errors.As(r.Error, &e1):
				cr.Err = &cachedError{Type: "decoding", Message: e1.Err.Error()}
			case errors.As(r.Error, &e2):
				cr.Err = &cachedError{Type: "unexpected_status", Code: e2.StatusCode}
			case errors.As(r.Error, &e3):
				cr.Err = &cachedError{Type: "network", Message: e3.Err.Error()}
			default:
				cr.Err = &cachedError{Type: "unknown", Message: e.Error()}
			}
		}
	}

	return cr
}

func (cr cachedResponse[T]) toResponse() Response[T] {
	r := Response[T]{
		Data:       cr.Data,
		ETag:       cr.ETag,
		StatusCode: cr.StatusCode,
		RateLimits: cr.RateLimits,
	}

	if cr.Err != nil {
		switch cr.Err.Type {
		case "api":
			r.Error = &APIError{
				StatusCode:       cr.Err.Code,
				Message:          cr.Err.Message,
				DocumentationURL: cr.Err.DocumentationURL,
			}
		case "decoding":
			r.Error = &DecodingError{Err: errors.New(cr.Err.Message)}
		case "unexpected_status":
			r.Error = &UnexpectedStatusError{StatusCode: cr.Err.Code}
		case "network":
			r.Error = &NetworkError{Err: errors.New(cr.Err.Message)}
		default:
			r.Error = errors.New(cr.Err.Message)
		}
	}

	return r
}

func getCache[T any](ctx context.Context, c Cache, cacheKey string) (Response[T], error) {
	if cached, err := c.Get(ctx, cacheKey); err == nil {
		var cr cachedResponse[T]
		if err := json.Unmarshal(cached, &cr); err == nil {
			return cr.toResponse(), nil
		}
	}

	return Response[T]{}, errors.New("cache miss")
}

func setCache[T any](ctx context.Context, c Cache, cacheKey string, r Response[T], ttl time.Duration) error {
	if r.StatusCode == 0 || ttl == 0 {
		return nil
	}

	if r.StatusCode == http.StatusNotFound {
		r.ETag = ""
	}

	if b, err := json.Marshal(toCached(r)); err == nil {
		return c.Set(ctx, cacheKey, b, ttl)
	}

	return nil
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
	if t.Cache != nil && req.Method == http.MethodGet {
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

		if b, err := json.Marshal(cached); err == nil {
			ttl := t.TTL
			if resp.StatusCode >= 400 {
				ttl = t.ErrorTTL
			}

			err = t.Cache.Set(ctx, cacheKey, b, ttl)
			if err != nil {
				log.Printf("cache transport: failed to set cache for %s: %v", cacheKey, err)
			}
		}
	}

	return resp, nil
}
