// Package github provides a client for interacting with the GitHub API.
package github

import (
	"context"
	"log"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// Client provides a client for interacting with the GitHub API.
type Client struct {
	token string

	httpClient *http.Client
	BaseURL    string

	cache         *redis.Client
	cacheTTL      time.Duration
	cacheErrorTTL time.Duration

	mu         sync.RWMutex
	lastLimits RateLimits
}

// Option defines a functional configuration type for the GitHub Client.
type Option func(*Client)

// WithToken sets the GitHub personal access token for authentication.
func WithToken(token string) Option {
	return func(c *Client) { c.token = token }
}

// WithBaseURL overrides the default GitHub API base URL.
func WithBaseURL(baseURL string) Option {
	return func(c *Client) { c.BaseURL = baseURL }
}

// WithHTTPClient sets a custom http.Client for the GitHub client.
func WithHTTPClient(httpClient *http.Client) Option {
	return func(c *Client) { c.httpClient = httpClient }
}

// WithCache configures a Redis-based cache for API responses.
func WithCache(client *redis.Client, ttl time.Duration, errorTTL time.Duration) Option {
	return func(c *Client) {
		c.cache = client
		c.cacheTTL = ttl
		c.cacheErrorTTL = errorTTL
	}
}

// WithInitialRateLimits seeds the client with starting rate limit values.
func WithInitialRateLimits(limits RateLimits) Option {
	return func(c *Client) { c.lastLimits = limits }
}

// NewClient creates a new Client with the provided options.
func NewClient(opts ...Option) *Client {
	c := &Client{
		httpClient: http.DefaultClient,
		BaseURL:    "https://api.github.com",
		lastLimits: RateLimits{Limit: -1, Remaining: -1},
	}

	for _, opt := range opts {
		opt(c)
	}

	return c
}

func (c *Client) getCachedRateLimits() RateLimits {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.lastLimits
}

func (c *Client) setCachedRateLimits(newLimits RateLimits) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !newLimits.IsValid() {
		if !newLimits.RetryAfter.IsZero() && newLimits.RetryAfter.After(c.lastLimits.RetryAfter) {
			c.lastLimits.RetryAfter = newLimits.RetryAfter
		}
		return
	}

	c.lastLimits.Limit = newLimits.Limit
	c.lastLimits.Remaining = newLimits.Remaining
	c.lastLimits.ResetAt = newLimits.ResetAt

	if newLimits.RetryAfter.After(c.lastLimits.RetryAfter) {
		c.lastLimits.RetryAfter = newLimits.RetryAfter
	}
}

// GetBaseRateLimits returns the default rate limits based on whether a token is configured.
func (c *Client) GetBaseRateLimits() RateLimits {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.token != "" {
		return RateLimits{Limit: 5000, Remaining: 5000, ResetAt: time.Now().Add(1 * time.Hour)}
	}

	return RateLimits{Limit: 60, Remaining: 60, ResetAt: time.Now().Add(1 * time.Hour)}
}

// GetRateLimits retrieves the current rate limits, either from the cache or by making an API request.
func (c *Client) GetRateLimits(ctx context.Context) RateLimits {
	cached := c.getCachedRateLimits()
	if cached.IsValid() {
		return cached
	}

	response := get(ctx, c, []string{"rate_limit"}, "", false, func(res *http.Response) (RateLimits, error) {
		return formatResponse[any](res, nil, nil).RateLimits, nil
	})
	return response.RateLimits
}

func get[T any](ctx context.Context, c *Client, path []string, etag string, cache bool, handler ResponseHandler[T]) Response[T] {
	var endpoint string

	if len(path) > 0 {
		u, err := url.Parse(c.BaseURL)
		if err != nil {
			return Response[T]{Error: err}
		}
		endpoint = u.JoinPath(path...).String()
	} else {
		endpoint = c.BaseURL
	}

	if c.httpClient == nil {
		c.httpClient = http.DefaultClient
	}

	var cacheKey string
	if cache && c.cache != nil {
		cacheKey = c.getCacheKey(endpoint)
		if resp, ok := tryGetCache[T](ctx, c, cacheKey); ok {
			return resp
		}
	}

	var data T

	req, err := http.NewRequestWithContext(ctx, "GET", endpoint, nil)
	if err != nil {
		return Response[T]{Error: &NetworkError{err}}
	}

	if etag != "" {
		req.Header.Set("If-None-Match", etag)
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}

	res, err := c.httpClient.Do(req)
	if err != nil {
		return Response[T]{Error: &NetworkError{err}}
	}
	defer func() {
		if cerr := res.Body.Close(); cerr != nil {
			log.Printf("error closing response body: %v", cerr)
		}
	}()

	data, execErr := handler(res)

	formattedResponse := formatResponse(res, data, execErr)
	if formattedResponse.RateLimits.IsValid() {
		c.setCachedRateLimits(formattedResponse.RateLimits)
	}

	if cache && c.cache != nil {
		trySetCache(ctx, c, cacheKey, formattedResponse)
	}

	return formattedResponse
}

// Repository represents a GitHub repository.
type Repository struct {
	ID       int64  `json:"id"`
	FullName string `json:"full_name"`
}

// GetRepository fetches information about a GitHub repository.
func (c *Client) GetRepository(ctx context.Context, owner, name, etag string) Response[Repository] {
	handler := CreateStatusHandler(jsonDecoder[Repository])
	return get(ctx, c, []string{"repos", owner, name}, etag, true, handler)
}

// LatestRelease represents the latest release of a GitHub repository.
type LatestRelease struct {
	ID      int64  `json:"id"`
	TagName string `json:"tag_name"`
	URL     string `json:"html_url"`
}

// GetLatestRelease fetches the latest release for a GitHub repository.
func (c *Client) GetLatestRelease(ctx context.Context, owner, name, etag string) Response[LatestRelease] {
	handler := CreateStatusHandler(jsonDecoder[LatestRelease])
	return get(ctx, c, []string{"repos", owner, name, "releases", "latest"}, etag, true, handler)
}
