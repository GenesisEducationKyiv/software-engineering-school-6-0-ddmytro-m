//go:build testing

package github

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Redis-GitHub API integration tests

var testRedis *redis.Client

func TestMain(m *testing.M) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	container, client, err := setupRedisContainer(ctx)
	if err != nil {
		log.Fatalf("critical failure: %v", err)
	}

	testRedis = client

	code := m.Run()

	err = container.Terminate(context.Background())
	if err != nil {
		log.Fatalf("container termination failure: %v", err)
	}

	os.Exit(code)
}

func setupRedisContainer(ctx context.Context) (testcontainers.Container, *redis.Client, error) {
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

	client := redis.NewClient(&redis.Options{Addr: endpoint})

	return redisC, client, nil
}

func getCleanRedis(t *testing.T) *redis.Client {
	t.Helper()
	testRedis.FlushAll(context.Background())
	return testRedis
}

func TestGet_CacheHit(t *testing.T) {
	rc := getCleanRedis(t)
	ctx := context.Background()

	srv, c := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		t.Error("HTTP server should not be called on cache hit")
	})
	c.cache = rc
	c.cacheTTL = 1 * time.Minute

	cachedResp := Response[LatestRelease]{
		Data:       LatestRelease{TagName: "v2.0.0"},
		StatusCode: 200,
	}
	b, _ := json.Marshal(toCached(cachedResp))
	rc.Set(ctx, "github_cache:"+srv.URL, b, c.cacheTTL)

	resp := get(ctx, c, []string{}, "", true, CreateStatusHandler(jsonDecoder[LatestRelease]))
	if resp.Data.TagName != "v2.0.0" {
		t.Errorf("expected TagName 'v2.0.0', got %q", resp.Data.TagName)
	}
}

func TestGet_CacheMiss_SetsCache(t *testing.T) {
	rc := getCleanRedis(t)
	ctx := context.Background()

	callCount := 0
	srv, c := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		callCount++
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"tag_name":"v3.0.0"}`))
	})
	c.cache = rc
	c.cacheTTL = 1 * time.Minute

	// First call misses cache, hits server
	resp := get(ctx, c, []string{}, "", true, CreateStatusHandler(jsonDecoder[LatestRelease]))
	if resp.Data.TagName != "v3.0.0" {
		t.Errorf("expected TagName 'v3.0.0', got %q", resp.Data.TagName)
	}
	if callCount != 1 {
		t.Errorf("expected 1 HTTP call, got %d", callCount)
	}

	// Verify it was set in Redis
	cachedData, err := rc.Get(ctx, "github_cache:"+srv.URL).Bytes()
	if err != nil {
		t.Fatalf("failed to get cache: %v", err)
	}
	var cachedResp cachedResponse[LatestRelease]
	if err := json.Unmarshal(cachedData, &cachedResp); err != nil {
		t.Fatalf("failed to unmarshal cache: %v", err)
	}
	if cachedResp.Data.TagName != "v3.0.0" {
		t.Errorf("cached TagName mismatch: got %q", cachedResp.Data.TagName)
	}

	// Second call hits cache, no extra HTTP call
	get(ctx, c, []string{}, "", true, CreateStatusHandler(jsonDecoder[LatestRelease]))
	if callCount != 1 {
		t.Errorf("expected 1 HTTP call after cache hit, got %d", callCount)
	}
}

func TestGet_CacheErrorTTL_On404(t *testing.T) {
	rc := getCleanRedis(t)
	ctx := context.Background()

	srv, c := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"some-etag"`)
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"message":"Not Found"}`))
	})
	c.cache = rc
	c.cacheErrorTTL = 1 * time.Minute

	get(ctx, c, []string{}, "", true, CreateStatusHandler(jsonDecoder[LatestRelease]))

	// Verify it was set in Redis with an empty ETag logic
	cachedData, err := rc.Get(ctx, "github_cache:"+srv.URL).Bytes()
	if err != nil {
		t.Fatalf("expected 404 response to be cached: %v", err)
	}
	var cachedResp cachedResponse[LatestRelease]
	if err := json.Unmarshal(cachedData, &cachedResp); err != nil {
		t.Fatalf("failed to unmarshal cache: %v", err)
	}

	if cachedResp.StatusCode != 404 {
		t.Errorf("expected cached status 404, got %d", cachedResp.StatusCode)
	}
	if cachedResp.ETag != "" {
		t.Errorf("expected empty ETag on cached 404, got %q", cachedResp.ETag)
	}
}

func TestGet_CacheErrorTTL_OnDefaultError(t *testing.T) {
	rc := getCleanRedis(t)
	ctx := context.Background()

	srv, c := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	c.cache = rc
	c.cacheErrorTTL = 1 * time.Minute

	get(ctx, c, []string{}, "", true, CreateStatusHandler(jsonDecoder[LatestRelease]))

	cachedData, err := rc.Get(ctx, "github_cache:"+srv.URL).Bytes()
	if err != nil {
		t.Fatalf("expected error response to be cached: %v", err)
	}
	var cachedResp cachedResponse[LatestRelease]
	if err := json.Unmarshal(cachedData, &cachedResp); err != nil {
		t.Fatalf("failed to unmarshal cache: %v", err)
	}

	if cachedResp.StatusCode != 500 {
		t.Errorf("expected cached status 500, got %d", cachedResp.StatusCode)
	}
}
