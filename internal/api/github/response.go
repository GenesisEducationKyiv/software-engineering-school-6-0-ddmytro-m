package github

import (
	"net/http"
	"time"
)

// Response is a generic response wrapper for the GitHub API.
type Response[T any] struct {
	Data T

	RateLimits RateLimits
	ETag       string

	StatusCode int
	Error      error `json:"-"`
}

func formatResponse[T any](res *http.Response, data T, err error) Response[T] {
	if res == nil {
		return Response[T]{Data: data, Error: err}
	}

	rateLimits := RateLimits{
		Limit:      getInt64Header(res.Header, "X-RateLimit-Limit"),
		Remaining:  getInt64Header(res.Header, "X-RateLimit-Remaining"),
		ResetAt:    getResetTime(res.Header),
		RetryAfter: getRetryAfterTime(res.Header, time.Now()),
	}

	return Response[T]{
		Data:       data,
		RateLimits: rateLimits,
		ETag:       res.Header.Get("ETag"),
		StatusCode: res.StatusCode,
		Error:      err,
	}
}
