//go:build integration

package redis

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

var testRedis *goredis.Client

func TestMain(m *testing.M) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	container, client, err := setupRedisContainer(ctx)
	if err != nil {
		log.Fatalf("critical failure setting up redis container: %v", err)
	}

	testRedis = client

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

func TestGetClient_Singleton(t *testing.T) {
	client1 := GetClient(testRedis.Options().Addr)
	client2 := GetClient(testRedis.Options().Addr)

	if client1 == nil {
		t.Fatal("GetClient() returned nil")
	}

	if client1 != client2 {
		t.Errorf("GetClient() is not a singleton, got different instances: %p and %p", client1, client2)
	}
}

func TestNewCache_UsesSingletonClient(t *testing.T) {
	cache := NewCacheWithAddr(testRedis.Options().Addr)
	if cache == nil {
		t.Fatal("NewCache() returned nil")
	}
	if cache.client != GetClient(testRedis.Options().Addr) {
		t.Errorf("expected NewCache() to use singleton GetClient()")
	}
}

func TestCache_SetAndGet(t *testing.T) {
	rc := getCleanRedis(t)
	cache := NewCacheWithClient(rc)
	ctx := context.Background()

	key := "integration-test-key"
	val := []byte("integration-test-value")

	if err := cache.Set(ctx, key, val, 1*time.Minute); err != nil {
		t.Fatalf("failed to set value: %v", err)
	}

	got, err := cache.Get(ctx, key)
	if err != nil {
		t.Fatalf("failed to get value: %v", err)
	}

	if string(got) != string(val) {
		t.Errorf("expected %q, got %q", val, got)
	}
}

func TestCache_GetNonExistent(t *testing.T) {
	rc := getCleanRedis(t)
	cache := NewCacheWithClient(rc)
	ctx := context.Background()

	_, err := cache.Get(ctx, "non-existent-key")
	if err != goredis.Nil {
		t.Errorf("expected redis.Nil error, got %v", err)
	}
}
