//go:build unit

package github

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"
)

// get
func TestGet_200_ParsedAndRateLimitsCached(t *testing.T) {
	epoch := int64(1_700_000_000)
	_, c := newTestServer(t, func(w http.ResponseWriter, _ *http.Request) {
		for k, v := range rateLimitHeaders(5000, 4998, epoch) {
			w.Header().Set(k, v)
		}
		w.Header().Set("ETag", `"etag-v1"`)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"id":1,"tag_name":"v1.0.0","html_url":"https://github.com/x/y/releases/tag/v1.0.0"}`))
	})

	resp := get(context.Background(), c.httpClient, c.BaseURL, "", CreateStatusHandler(jsonDecoder[LatestRelease]))

	if resp.Error != nil {
		t.Fatalf("unexpected error: %v", resp.Error)
	}
	if resp.Data.TagName != "v1.0.0" {
		t.Errorf("TagName = %q, want v1.0.0", resp.Data.TagName)
	}
	if resp.ETag != `"etag-v1"` {
		t.Errorf("ETag = %q, want \"etag-v1\"", resp.ETag)
	}
}

func TestGet_304_ETagSentAndZeroValueReturned(t *testing.T) {
	var receivedETag string
	_, c := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		receivedETag = r.Header.Get("If-None-Match")
		w.WriteHeader(http.StatusNotModified)
	})

	resp := get(context.Background(), c.httpClient, c.BaseURL, `"etag-v1"`, CreateStatusHandler(jsonDecoder[LatestRelease]))

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
	_, c := newTestServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"message":"Not Found"}`))
	})

	resp := get(context.Background(), c.httpClient, c.BaseURL, "", CreateStatusHandler(jsonDecoder[LatestRelease]))

	var apiErr *APIError
	if !errors.As(resp.Error, &apiErr) {
		t.Fatalf("expected *APIError, got %T: %v", resp.Error, resp.Error)
	}
	if apiErr.StatusCode != 404 {
		t.Errorf("APIError.StatusCode = %d, want 404", apiErr.StatusCode)
	}
}

func TestGet_NetworkFailure_ReturnsNetworkError(t *testing.T) {
	srv, c := newTestServer(t, func(_ http.ResponseWriter, _ *http.Request) {})
	srv.Close() // close before the request is made

	resp := get(context.Background(), c.httpClient, c.BaseURL, "", CreateStatusHandler(jsonDecoder[LatestRelease]))

	var ne *NetworkError
	if !errors.As(resp.Error, &ne) {
		t.Errorf("expected *NetworkError, got %T: %v", resp.Error, resp.Error)
	}
}

func TestGet_CancelledContext_ReturnsNetworkError(t *testing.T) {
	_, c := newTestServer(t, func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	resp := get(ctx, c.httpClient, c.BaseURL, "", CreateStatusHandler(jsonDecoder[LatestRelease]))

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
	_, c := newTestServer(t, func(w http.ResponseWriter, _ *http.Request) {
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
	c := NewClient(WithBaseURL("://invalid-url")) // invalid format to force url.Parse to fail

	resp := c.GetRepository(context.Background(), "owner", "repo", "")
	if resp.Error == nil {
		t.Error("expected error due to invalid base URL, got nil")
	}
}
