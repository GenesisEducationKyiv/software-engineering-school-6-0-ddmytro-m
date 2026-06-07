//go:build integration

package mailer

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/mq"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/redis"
	goredis "github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Mailer-Redis integration tests

var testRedis *goredis.Client

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

func TestProcessMessage_ValidEvents(t *testing.T) {
	rc := getCleanRedis(t)
	ctx := context.Background()

	stream := redis.NewStream(rc, "test_stream")
	mailer := NewMailer(stream, "test_group", 1, nil)

	if err := mailer.stream.EnsureConsumerGroup(ctx, mailer.group); err != nil {
		t.Fatalf("failed to ensure consumer group: %v", err)
	}

	events := []struct {
		name  string
		event string
	}{
		{"NewRelease", string(mq.EventNewRelease)},
		{"RepoMoved", string(mq.EventRepoMoved)},
		{"EmailVerification", string(mq.EventEmailVerification)},
	}

	for _, tc := range events {
		t.Run(tc.name, func(t *testing.T) {
			jsonPayload := fmt.Sprintf(`{"event": "%s", "email": "test@example.com", "repo": "owner/repo", "release": "v1.0.0", "payload": {"token": "12345"}}`, tc.event)

			msg := redis.NewMessage(goredis.XMessage{
				ID:     "1-0",
				Values: map[string]any{"payload": jsonPayload},
			})

			mailer.processMessage(ctx, 1, msg)
		})
	}
}

func TestProcessMessage_InvalidPayloadType(t *testing.T) {
	rc := getCleanRedis(t)
	ctx := context.Background()

	stream := redis.NewStream(rc, "test_stream")
	mailer := NewMailer(stream, "test_group", 1, nil)

	msg := redis.NewMessage(
		goredis.XMessage{
			ID: "1-0",
			Values: map[string]any{
				"payload": 123, // invalid type
			},
		},
	)

	mailer.processMessage(ctx, 1, msg)
}

func TestProcessMessage_InvalidJSON(t *testing.T) {
	rc := getCleanRedis(t)
	ctx := context.Background()

	stream := redis.NewStream(rc, "test_stream")
	mailer := NewMailer(stream, "test_group", 1, nil)

	msg := redis.NewMessage(
		goredis.XMessage{
			ID: "2-0",
			Values: map[string]any{
				"payload": `{"event": "broken`, // invalid JSON
			},
		},
	)

	mailer.processMessage(ctx, 1, msg)
}

func TestMailer_StartAndConsume(t *testing.T) {
	rc := getCleanRedis(t)
	ctx, cancel := context.WithCancel(context.Background())

	stream := redis.NewStream(rc, "test_stream")
	mailer := NewMailer(stream, "test_group", 2, nil)

	if err := stream.EnsureConsumerGroup(ctx, "test_group"); err != nil {
		t.Logf("EnsureConsumerGroup error: %v", err)
	}

	jsonPayload := fmt.Sprintf(`{"event": "%s", "email": "test@example.com", "repo": "owner/repo", "release": "v1.0.0"}`, mq.EventNewRelease)

	err := rc.XAdd(ctx, &goredis.XAddArgs{
		Stream: "test_stream",
		Values: map[string]any{
			"payload": jsonPayload,
		},
	}).Err()
	if err != nil {
		t.Fatalf("failed to add message to stream: %v", err)
	}

	done := make(chan struct{})
	go func() {
		mailer.Start(ctx)
		close(done)
	}()

	time.Sleep(500 * time.Millisecond)
	cancel()
	<-done

	pending, err := rc.XPending(context.Background(), "test_stream", "test_group").Result()
	if err == nil {
		if pending.Count != 0 {
			t.Errorf("expected 0 pending messages, got %d", pending.Count)
		}
	}
}
