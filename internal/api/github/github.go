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

type GitHubClient struct {
	token string

	httpClient *http.Client
	BaseURL    string

	cache         *redis.Client
	cacheTTL      time.Duration
	cacheErrorTTL time.Duration

	mu         sync.RWMutex
	lastLimits RateLimits
}

func NewGitHubClient(token string, httpClient *http.Client, cache *redis.Client, cacheTTL time.Duration, cacheErrorTTL time.Duration) *GitHubClient {
	return &GitHubClient{
		token: token,

		httpClient: httpClient,
		BaseURL:    "https://api.github.com",

		cache:         cache,
		cacheTTL:      cacheTTL,
		cacheErrorTTL: cacheErrorTTL,

		mu:         sync.RWMutex{},
		lastLimits: RateLimits{Limit: -1, Remaining: -1},
	}
}

func (c *GitHubClient) getCachedRateLimits() RateLimits {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.lastLimits
}

func (c *GitHubClient) setCachedRateLimits(newLimits RateLimits) {
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

func (c *GitHubClient) GetBaseRateLimits() RateLimits {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.token != "" {
		return RateLimits{Limit: 5000, Remaining: 5000, ResetAt: time.Now().Add(1 * time.Hour)}
	} else {
		return RateLimits{Limit: 60, Remaining: 60, ResetAt: time.Now().Add(1 * time.Hour)}
	}
}

func (c *GitHubClient) GetRateLimits(ctx context.Context) RateLimits {
	cached := c.getCachedRateLimits()
	if cached.IsValid() {
		return cached
	}

	response := get(ctx, c, []string{"rate_limit"}, "", false, func(res *http.Response) (RateLimits, error) {
		return formatResponse[any](res, nil, nil).RateLimits, nil
	})
	return response.RateLimits
}

func get[T any](ctx context.Context, c *GitHubClient, path []string, etag string, cache bool, handler ResponseHandler[T]) GitHubResponse[T] {
	var endpoint string

	if len(path) > 0 {
		u, err := url.Parse(c.BaseURL)
		if err != nil {
			return GitHubResponse[T]{Error: err}
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
		return GitHubResponse[T]{Error: &NetworkError{err}}
	}

	if etag != "" {
		req.Header.Set("If-None-Match", etag)
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}

	res, err := c.httpClient.Do(req)
	if err != nil {
		return GitHubResponse[T]{Error: &NetworkError{err}}
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

type Repository struct {
	ID       int64  `json:"id"`
	FullName string `json:"full_name"`
}

func (c *GitHubClient) GetRepository(ctx context.Context, owner, name, etag string) GitHubResponse[Repository] {
	handler := CreateStatusHandler(jsonDecoder[Repository])
	return get(ctx, c, []string{"repos", owner, name}, etag, true, handler)
}

type LatestRelease struct {
	ID      int64  `json:"id"`
	TagName string `json:"tag_name"`
	URL     string `json:"html_url"`
}

func (c *GitHubClient) GetLatestRelease(ctx context.Context, owner, name, etag string) GitHubResponse[LatestRelease] {
	handler := CreateStatusHandler(jsonDecoder[LatestRelease])
	return get(ctx, c, []string{"repos", owner, name, "releases", "latest"}, etag, true, handler)
}
