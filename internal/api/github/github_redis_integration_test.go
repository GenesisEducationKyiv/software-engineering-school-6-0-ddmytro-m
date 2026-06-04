//go:build integration

package github

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Redis-GitHub API integration tests

var testCache Cache
var testRedis *goredis.Client

type redisCache struct {
	client *goredis.Client
}

func (c *redisCache) Get(ctx context.Context, key string) ([]byte, error) {
	return c.client.Get(ctx, key).Bytes()
}

func (c *redisCache) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	return c.client.Set(ctx, key, value, ttl).Err()
}

func TestMain(m *testing.M) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	container, client, err := setupRedisContainer(ctx)
	if err != nil {
		log.Fatalf("critical failure: %v", err)
	}

	testRedis = client
	testCache = &redisCache{client: client}

	code := m.Run()

	err = container.Terminate(context.Background())
	if err != nil {
		log.Fatalf("container termination failure: %v", err)
	}

	os.Exit(code)
}

func setupRedisContainer(ctx context.Context) (testcontainers.Container, *goredis.Client, error) {
	req := testcontainers.ContainerRequest{
		Image:        "redis:7-alpine",
		ExposedPorts: []string{"6379/tcp"},
		WaitingFor:   wait.ForListeningPort("6379/tcp"),
	}

	redisC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})

	if err != nil {
		return nil, nil, err
	}

	endpoint, err := redisC.Endpoint(ctx, "")
	if err != nil {
		return nil, nil, err
	}

	client := goredis.NewClient(&goredis.Options{Addr: endpoint})

	return redisC, client, nil
}

func getCleanRedis(t *testing.T) *goredis.Client {
	t.Helper()
	testRedis.FlushAll(context.Background())
	return testRedis
}

func TestCacheTransport_CacheHit(t *testing.T) {
	rc := getCleanRedis(t)
	ctx := context.Background()

	endpoint := "https://api.github.com/repos/owner/repo/releases/latest"
	cacheKey := getCacheKey(endpoint)

	// Manually seed Redis with the raw HTTP response structure
	cachedResp := cachedHTTPResponse{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       []byte(`{"tag_name":"v2.0.0"}`),
	}
	b, err := json.Marshal(cachedResp)
	if err != nil {
		t.Fatalf("failed to marshal cached response: %v", err)
	}
	rc.Set(ctx, cacheKey, b, time.Minute)

	// Setup transport that FATALS if the network is hit
	failTransport := &mockTransport{
		roundTripFunc: func(req *http.Request) (*http.Response, error) {
			t.Fatal("HTTP server should not be called on cache hit")
			return nil, nil
		},
	}

	transport := &CacheTransport{
		Transport: failTransport,
		Cache:     &redisCache{client: rc},
		TTL:       time.Minute,
	}
	client := &http.Client{Transport: transport}

	// Execute
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()

	// Assert
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read response body: %v", err)
	}

	if string(bodyBytes) != `{"tag_name":"v2.0.0"}` {
		t.Errorf("expected cached body, got %q", string(bodyBytes))
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}
}

func TestCacheTransport_CacheMiss_SetsCache(t *testing.T) {
	rc := getCleanRedis(t)
	ctx := context.Background()

	callCount := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"tag_name":"v3.0.0"}`))
	}))
	defer srv.Close()

	transport := &CacheTransport{
		Transport: http.DefaultTransport,
		Cache:     &redisCache{client: rc},
		TTL:       time.Minute,
	}
	client := &http.Client{Transport: transport}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// First call misses cache, hits server
	resp1, err := client.Do(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp1.Body.Close()

	if callCount != 1 {
		t.Errorf("expected 1 HTTP call, got %d", callCount)
	}

	// Verify it was set in Redis as a cachedHTTPResponse
	cachedData, err := rc.Get(ctx, getCacheKey(srv.URL)).Bytes()
	if err != nil {
		t.Fatalf("failed to get cache: %v", err)
	}
	var cachedResp cachedHTTPResponse
	if err := json.Unmarshal(cachedData, &cachedResp); err != nil {
		t.Fatalf("failed to unmarshal cache: %v", err)
	}
	if string(cachedResp.Body) != `{"tag_name":"v3.0.0"}` {
		t.Errorf("cached Body mismatch: got %q", string(cachedResp.Body))
	}

	// Second call hits cache
	resp2, err := client.Do(req)
	if err != nil {
		t.Fatalf("unexpected error on second call: %v", err)
	}
	resp2.Body.Close()

	if callCount != 1 {
		t.Errorf("expected 1 HTTP call after cache hit, got %d", callCount)
	}
}

func TestCacheTransport_CacheErrorTTL_On404(t *testing.T) {
	rc := getCleanRedis(t)
	ctx := context.Background()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"some-etag"`)
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"message":"Not Found"}`))
	}))
	defer srv.Close()

	transport := &CacheTransport{
		Transport: http.DefaultTransport,
		Cache:     &redisCache{client: rc},
		TTL:       time.Minute,
		ErrorTTL:  10 * time.Minute,
	}
	client := &http.Client{Transport: transport}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	resp.Body.Close()

	// Verify it was set in Redis
	cachedData, err := rc.Get(ctx, getCacheKey(srv.URL)).Bytes()
	if err != nil {
		t.Fatalf("expected 404 response to be cached (ensure cache.go doesn't block errors!): %v", err)
	}

	var cachedResp cachedHTTPResponse
	if err := json.Unmarshal(cachedData, &cachedResp); err != nil {
		t.Fatalf("failed to unmarshal cache: %v", err)
	}

	if cachedResp.StatusCode != 404 {
		t.Errorf("expected cached status 404, got %d", cachedResp.StatusCode)
	}
}

func TestCacheTransport_CacheErrorTTL_OnDefaultError(t *testing.T) {
	rc := getCleanRedis(t)
	ctx := context.Background()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"message":"Server Error"}`))
	}))
	defer srv.Close()

	transport := &CacheTransport{
		Transport: http.DefaultTransport,
		Cache:     &redisCache{client: rc},
		TTL:       time.Minute,
		ErrorTTL:  10 * time.Minute,
	}
	client := &http.Client{Transport: transport}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	resp.Body.Close()

	cachedData, err := rc.Get(ctx, getCacheKey(srv.URL)).Bytes()
	if err != nil {
		t.Fatalf("expected 500 response to be cached: %v", err)
	}

	var cachedResp cachedHTTPResponse
	if err := json.Unmarshal(cachedData, &cachedResp); err != nil {
		t.Fatalf("failed to unmarshal cache: %v", err)
	}

	if cachedResp.StatusCode != 500 {
		t.Errorf("expected cached status 500, got %d", cachedResp.StatusCode)
	}
}
