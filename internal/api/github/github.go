// Package github provides a client for interacting with the GitHub API.
package github

import (
	"context"
	"fmt"
	"net/http"

	"go.uber.org/zap"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/logger"
)

// Client provides a client for interacting with the GitHub API.
type Client struct {
	httpClient *http.Client
	BaseURL    string
}

// Option defines a functional configuration type for the GitHub Client.
type Option func(*Client)

// WithBaseURL overrides the default GitHub API base URL.
func WithBaseURL(baseURL string) Option {
	return func(c *Client) { c.BaseURL = baseURL }
}

// WithHTTPClient sets a custom http.Client for the GitHub client.
func WithHTTPClient(httpClient *http.Client) Option {
	return func(c *Client) { c.httpClient = httpClient }
}

// NewClient creates a new Client with the provided options.
func NewClient(opts ...Option) *Client {
	c := &Client{
		httpClient: http.DefaultClient,
		BaseURL:    "https://api.github.com",
	}

	for _, opt := range opts {
		opt(c)
	}

	return c
}

func get[T any](ctx context.Context, c *http.Client, endpoint string, etag string, handler ResponseHandler[T]) Response[T] {
	if c == nil {
		c = http.DefaultClient
	}

	req, err := http.NewRequestWithContext(ctx, "GET", endpoint, nil)
	if err != nil {
		return Response[T]{Error: &NetworkError{err}}
	}

	if etag != "" {
		req.Header.Set("If-None-Match", etag)
	}

	res, err := c.Do(req)
	if err != nil {
		return Response[T]{Error: &NetworkError{err}}
	}
	defer func() {
		if cerr := res.Body.Close(); cerr != nil {
			logger.Log.Error("error closing response body", zap.Error(cerr))
		}
	}()

	data, execErr := handler(res)

	formattedResponse := formatResponse(res, data, execErr)

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
	endpoint := fmt.Sprintf("%s/repos/%s/%s", c.BaseURL, owner, name)
	return get(ctx, c.httpClient, endpoint, etag, handler)
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
	endpoint := fmt.Sprintf("%s/repos/%s/%s/releases/latest", c.BaseURL, owner, name)
	return get(ctx, c.httpClient, endpoint, etag, handler)
}
