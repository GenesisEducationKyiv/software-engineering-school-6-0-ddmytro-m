//go:build unit || integration

package github

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
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

// newTestServer starts an httptest.Server and returns a Client whose
// httpClient is pre-configured to talk to it. The server is closed via t.Cleanup.
func newTestServer(t *testing.T, handler http.HandlerFunc) (*httptest.Server, *Client) {
	t.Helper()

	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	c := NewClient(
		WithToken("test-token"),
		WithHTTPClient(srv.Client()),
		WithBaseURL(srv.URL),
	)

	return srv, c
}
