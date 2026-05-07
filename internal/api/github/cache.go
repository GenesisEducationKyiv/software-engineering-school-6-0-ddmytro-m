package github

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
)

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

func tryGetCache[T any](ctx context.Context, c *Client, cacheKey string) (Response[T], bool) {
	if cached, err := c.cache.Get(ctx, cacheKey).Bytes(); err == nil {
		var cr cachedResponse[T]
		if err := json.Unmarshal(cached, &cr); err == nil {
			return cr.toResponse(), true
		}
	}
	return Response[T]{}, false
}

func trySetCache[T any](ctx context.Context, c *Client, cacheKey string, r Response[T]) {
	if r.StatusCode == 0 {
		return
	}

	ttl := c.cacheTTL
	if r.StatusCode != http.StatusOK && r.StatusCode != http.StatusNotModified {
		ttl = c.cacheErrorTTL
	}
	if ttl == 0 {
		return
	}

	if r.StatusCode == http.StatusNotFound {
		r.ETag = ""
	}

	if b, err := json.Marshal(toCached(r)); err == nil {
		c.cache.Set(ctx, cacheKey, b, ttl)
	}
}
