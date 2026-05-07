package github

import (
	"net/http"
	"strconv"
	"time"
)

// RateLimits represents the GitHub API rate limits.
type RateLimits struct {
	Limit      int64
	Remaining  int64
	ResetAt    time.Time
	RetryAfter time.Time
}

// IsValid checks if the rate limits are correctly initialized.
func (rl *RateLimits) IsValid() bool {
	return rl.Limit != -1 && rl.Remaining != -1 && !rl.ResetAt.IsZero()
}

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

func getInt64Header(h http.Header, key string) int64 {
	val := h.Get(key)
	if val == "" {
		return -1
	}

	parsed, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return -1
	}

	return parsed
}

func getResetTime(h http.Header) time.Time {
	val := h.Get("X-RateLimit-Reset")
	if val == "" {
		return time.Time{}
	}

	resetUnix, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return time.Time{}
	}

	return time.Unix(resetUnix, 0)
}

func getRetryAfterTime(h http.Header, now time.Time) time.Time {
	val := h.Get("Retry-After")
	if val == "" {
		return time.Time{}
	}

	// time in seconds
	if seconds, err := strconv.ParseInt(val, 10, 64); err == nil {
		return now.Add(time.Duration(seconds) * time.Second)
	}

	// time as http date
	if t, err := http.ParseTime(val); err == nil {
		return t
	}

	return time.Time{}
}
