package github

import (
	"context"
	"encoding/json"
	"errors"
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

func (c *Client) getCacheKey(endpoint string) string {
	return "github_cache:" + endpoint
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
