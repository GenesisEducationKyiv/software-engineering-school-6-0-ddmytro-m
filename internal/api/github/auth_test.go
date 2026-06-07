//go:build unit

package github

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAuthTransport_AddsAuthorizationHeader(t *testing.T) {
	var gotAuth string

	// Setup mock server to capture the header
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	// Configure the AuthTransport with a token
	authTransport := &AuthTransport{
		Transport: http.DefaultTransport,
		Token:     "test-token",
	}
	client := &http.Client{Transport: authTransport}

	// Make the request
	req, _ := http.NewRequest(http.MethodGet, server.URL, nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	resp.Body.Close()

	// Assert
	if want := "Bearer test-token"; gotAuth != want {
		t.Errorf("Authorization = %q, want %q", gotAuth, want)
	}
}

func TestAuthTransport_OmittedWhenNoToken(t *testing.T) {
	var gotAuth string

	// Setup mock server to capture the header
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	// Configure the AuthTransport WITHOUT a token
	authTransport := &AuthTransport{
		Transport: http.DefaultTransport,
		Token:     "", // Empty token
	}
	client := &http.Client{Transport: authTransport}

	// Make the request
	req, _ := http.NewRequest(http.MethodGet, server.URL, nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	resp.Body.Close()

	// Assert
	if gotAuth != "" {
		t.Errorf("expected no Authorization header, got %q", gotAuth)
	}
}

func TestAuthTransport_FallbackToDefaultTransport(t *testing.T) {
	var gotAuth string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	authTransport := &AuthTransport{
		Transport: nil, // Should fallback to http.DefaultTransport
		Token:     "test-token",
	}
	client := &http.Client{Transport: authTransport}

	req, _ := http.NewRequest(http.MethodGet, server.URL, nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	resp.Body.Close()

	if want := "Bearer test-token"; gotAuth != want {
		t.Errorf("Authorization = %q, want %q", gotAuth, want)
	}
}

func TestAuthTransport_DoesNotModifyOriginalRequest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	authTransport := &AuthTransport{
		Transport: http.DefaultTransport,
		Token:     "test-token",
	}

	req, _ := http.NewRequest(http.MethodGet, server.URL, nil)

	resp, err := authTransport.RoundTrip(req)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	resp.Body.Close()

	// The original request should not have the Authorization header
	if got := req.Header.Get("Authorization"); got != "" {
		t.Errorf("Original request was modified: got Authorization = %q", got)
	}
}
