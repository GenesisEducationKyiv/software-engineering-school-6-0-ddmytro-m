package github

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"
)

// fakeResponse builds an *http.Response through httptest.ResponseRecorder so
// headers are set before WriteHeader, matching real HTTP semantics.
func fakeResponse(status int, body string, headers map[string]string) *http.Response {
	w := httptest.NewRecorder()
	for k, v := range headers {
		w.Header().Set(k, v)
	}
	w.WriteHeader(status)
	if body != "" {
		_, _ = w.Write([]byte(body))
	}
	return w.Result()
}

func rateLimitHeaders(limit, remaining, resetUnix int64) map[string]string {
	return map[string]string{
		"X-RateLimit-Limit":     strconv.FormatInt(limit, 10),
		"X-RateLimit-Remaining": strconv.FormatInt(remaining, 10),
		"X-RateLimit-Reset":     strconv.FormatInt(resetUnix, 10),
	}
}

// newTestServer starts an httptest.Server and returns a GitHubClient whose
// httpClient is pre-configured to talk to it. The server is closed via t.Cleanup.
func newTestServer(t *testing.T, handler http.HandlerFunc) (*httptest.Server, *GitHubClient) {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	c := NewGitHubClient("test-token", srv.Client(), nil, time.Duration(0), time.Duration(0))
	c.BaseURL = srv.URL

	return srv, c
}

// internal limits
func TestUpdateInternalLimits_StoresValidLimits(t *testing.T) {
	c := NewGitHubClient("", nil, nil, time.Duration(0), time.Duration(0))
	reset := time.Now().Add(time.Hour).Truncate(time.Second)

	c.setCachedRateLimits(RateLimits{Limit: 5000, Remaining: 4000, ResetAt: reset})

	got := c.getCachedRateLimits()
	if got.Limit != 5000 || got.Remaining != 4000 {
		t.Errorf("unexpected limits after update: %+v", got)
	}
	if !got.ResetAt.Equal(reset) {
		t.Errorf("ResetAt = %v, want %v", got.ResetAt, reset)
	}
}

func TestUpdateInternalLimits_AllMissingIsIgnored(t *testing.T) {
	c := NewGitHubClient("", nil, nil, time.Duration(0), time.Duration(0))
	reset := time.Now().Add(time.Hour)
	c.setCachedRateLimits(RateLimits{Limit: 5000, Remaining: 4000, ResetAt: reset})

	c.setCachedRateLimits(RateLimits{Limit: -1, Remaining: -1})

	if got := c.getCachedRateLimits(); got.Limit != 5000 {
		t.Errorf("limits should not be overwritten by all-missing update; got Limit=%d", got.Limit)
	}
}

func TestUpdateInternalLimits_RetryAfterOnlyMovesForward(t *testing.T) {
	c := NewGitHubClient("", nil, nil, time.Duration(0), time.Duration(0))
	later := time.Now().Add(90 * time.Second)
	earlier := time.Now().Add(30 * time.Second)

	c.setCachedRateLimits(RateLimits{Limit: -1, Remaining: -1, RetryAfter: later})
	c.setCachedRateLimits(RateLimits{Limit: -1, Remaining: -1, RetryAfter: earlier})

	got := c.getCachedRateLimits()
	if !got.RetryAfter.Equal(later) {
		t.Errorf("RetryAfter should stay at later value; got %v, want %v", got.RetryAfter, later)
	}
}

// get
func TestGet_200_ParsedAndRateLimitsCached(t *testing.T) {
	epoch := int64(1_700_000_000)
	_, c := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		for k, v := range rateLimitHeaders(5000, 4998, epoch) {
			w.Header().Set(k, v)
		}
		w.Header().Set("ETag", `"etag-v1"`)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"id":1,"tag_name":"v1.0.0","html_url":"https://github.com/x/y/releases/tag/v1.0.0"}`))
	})

	resp := get(context.Background(), c, []string{}, "", false, CreateStatusHandler(jsonDecoder[LatestRelease]))

	if resp.Error != nil {
		t.Fatalf("unexpected error: %v", resp.Error)
	}
	if resp.Data.TagName != "v1.0.0" {
		t.Errorf("TagName = %q, want v1.0.0", resp.Data.TagName)
	}
	if resp.ETag != `"etag-v1"` {
		t.Errorf("ETag = %q, want \"etag-v1\"", resp.ETag)
	}

	limits := c.GetRateLimits(context.Background())
	if limits.Remaining != 4998 {
		t.Errorf("cached Remaining = %d, want 4998", limits.Remaining)
	}
	if !limits.ResetAt.Equal(time.Unix(epoch, 0)) {
		t.Errorf("cached ResetAt = %v, want %v", limits.ResetAt, time.Unix(epoch, 0))
	}
}

func TestGet_304_ETagSentAndZeroValueReturned(t *testing.T) {
	var receivedETag string
	_, c := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		receivedETag = r.Header.Get("If-None-Match")
		w.WriteHeader(http.StatusNotModified)
	})

	resp := get(context.Background(), c, []string{}, `"etag-v1"`, false, CreateStatusHandler(jsonDecoder[LatestRelease]))

	if resp.Error != nil {
		t.Fatalf("unexpected error on 304: %v", resp.Error)
	}
	if resp.StatusCode != 304 {
		t.Errorf("StatusCode = %d, want 304", resp.StatusCode)
	}
	if receivedETag != `"etag-v1"` {
		t.Errorf("server received If-None-Match = %q, want \"etag-v1\"", receivedETag)
	}
	if resp.Data.TagName != "" {
		t.Errorf("expected zero-value data on 304, got %+v", resp.Data)
	}
}

func TestGet_404_ReturnsAPIError(t *testing.T) {
	_, c := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"message":"Not Found"}`))
	})

	resp := get(context.Background(), c, []string{}, "", false, CreateStatusHandler(jsonDecoder[LatestRelease]))

	var apiErr *APIError
	if !errors.As(resp.Error, &apiErr) {
		t.Fatalf("expected *APIError, got %T: %v", resp.Error, resp.Error)
	}
	if apiErr.StatusCode != 404 {
		t.Errorf("APIError.StatusCode = %d, want 404", apiErr.StatusCode)
	}
}

func TestGet_AuthorizationHeader_SentWhenTokenPresent(t *testing.T) {
	var gotAuth string
	_, c := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"tag_name":"v1"}`))
	})

	get(context.Background(), c, []string{}, "", false, CreateStatusHandler(jsonDecoder[LatestRelease]))

	if want := "Bearer test-token"; gotAuth != want {
		t.Errorf("Authorization = %q, want %q", gotAuth, want)
	}
}

func TestGet_AuthorizationHeader_OmittedWhenNoToken(t *testing.T) {
	var gotAuth string
	_, c := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"tag_name":"v1"}`))
	})
	c.token = ""

	get(context.Background(), c, []string{}, "", false, CreateStatusHandler(jsonDecoder[LatestRelease]))

	if gotAuth != "" {
		t.Errorf("expected no Authorization header, got %q", gotAuth)
	}
}

func TestGet_NetworkFailure_ReturnsNetworkError(t *testing.T) {
	srv, c := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {})
	srv.Close() // close before the request is made

	resp := get(context.Background(), c, []string{}, "", false, CreateStatusHandler(jsonDecoder[LatestRelease]))

	var ne *NetworkError
	if !errors.As(resp.Error, &ne) {
		t.Errorf("expected *NetworkError, got %T: %v", resp.Error, resp.Error)
	}
}

func TestGet_CancelledContext_ReturnsNetworkError(t *testing.T) {
	_, c := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	resp := get(ctx, c, []string{}, "", false, CreateStatusHandler(jsonDecoder[LatestRelease]))

	var ne *NetworkError
	if !errors.As(resp.Error, &ne) {
		t.Errorf("expected *NetworkError from cancelled context, got %T: %v", resp.Error, resp.Error)
	}
}

func TestGetRepository_200_ParsesData(t *testing.T) {
	_, c := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/repos/owner/repo" {
			t.Errorf("expected path /repos/owner/repo, got %q", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"id":123,"full_name":"owner/repo"}`))
	})

	resp := c.GetRepository(context.Background(), "owner", "repo", "")

	if resp.Error != nil {
		t.Fatalf("unexpected error: %v", resp.Error)
	}
	if resp.Data.ID != 123 {
		t.Errorf("ID = %d, want 123", resp.Data.ID)
	}
	if resp.Data.FullName != "owner/repo" {
		t.Errorf("FullName = %q, want owner/repo", resp.Data.FullName)
	}
}

func TestGetRepository_304_PassesETag(t *testing.T) {
	_, c := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("If-None-Match") != `"etag-1"` {
			t.Errorf("expected ETag \"etag-1\", got %q", r.Header.Get("If-None-Match"))
		}
		w.WriteHeader(http.StatusNotModified)
	})

	resp := c.GetRepository(context.Background(), "owner", "repo", `"etag-1"`)

	if resp.Error != nil {
		t.Fatalf("unexpected error: %v", resp.Error)
	}
	if resp.StatusCode != 304 {
		t.Errorf("StatusCode = %d, want 304", resp.StatusCode)
	}
}

func TestGetRepository_404_ReturnsAPIError(t *testing.T) {
	_, c := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"message":"Not Found"}`))
	})

	resp := c.GetRepository(context.Background(), "owner", "repo", "")

	var apiErr *APIError
	if !errors.As(resp.Error, &apiErr) {
		t.Fatalf("expected *APIError, got %T: %v", resp.Error, resp.Error)
	}
}

func TestGetRepository_InvalidBaseURL(t *testing.T) {
	c := NewGitHubClient("", nil, nil, time.Duration(0), time.Duration(0))
	c.BaseURL = "://invalid-url" // invalid format to force url.Parse to fail

	resp := c.GetRepository(context.Background(), "owner", "repo", "")
	if resp.Error == nil {
		t.Error("expected error due to invalid base URL, got nil")
	}
}
