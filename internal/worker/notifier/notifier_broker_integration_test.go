//go:build integration

package notifier

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.uber.org/zap"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/events"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/mq"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/rabbitmq"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/redis"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/logger"
)

var (
	testAMQPURL string
	testRedis   *goredis.Client
)

func TestMain(m *testing.M) {
	logger.Log = zap.NewNop()
	ctx := context.Background()

	rabbitC, url, err := startRabbit(ctx)
	if err != nil {
		log.Fatalf("start rabbitmq: %v", err)
	}
	redisC, rc, err := startRedis(ctx)
	if err != nil {
		log.Fatalf("start redis: %v", err)
	}
	testAMQPURL = url
	testRedis = rc

	code := m.Run()

	_ = rabbitC.Terminate(context.Background())
	_ = redisC.Terminate(context.Background())
	os.Exit(code)
}

func startRabbit(ctx context.Context) (testcontainers.Container, string, error) {
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "rabbitmq:3-management-alpine",
			ExposedPorts: []string{"5672/tcp"},
			WaitingFor:   wait.ForLog("Server startup complete").WithStartupTimeout(120 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		return nil, "", err
	}
	host, err := c.Host(ctx)
	if err != nil {
		return nil, "", err
	}
	port, err := c.MappedPort(ctx, "5672/tcp")
	if err != nil {
		return nil, "", err
	}
	return c, fmt.Sprintf("amqp://guest:guest@%s:%s/", host, port.Port()), nil
}

func startRedis(ctx context.Context) (testcontainers.Container, *goredis.Client, error) {
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "redis:7-alpine",
			ExposedPorts: []string{"6379/tcp"},
			WaitingFor:   wait.ForListeningPort("6379/tcp"),
		},
		Started: true,
	})
	if err != nil {
		return nil, nil, err
	}
	endpoint, err := c.Endpoint(ctx, "")
	if err != nil {
		return nil, nil, err
	}
	return c, goredis.NewClient(&goredis.Options{Addr: endpoint}), nil
}

// startNotifier purges state and starts a notifier consumer on a fresh connection.
func startNotifier(ctx context.Context, t *testing.T) *rabbitmq.Connection {
	t.Helper()
	retry := rabbitmq.NewRetryPolicy(time.Second, 2, 3)
	conn, err := rabbitmq.Dial(testAMQPURL, retry)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	purge(t, conn)
	testRedis.FlushAll(ctx)

	n := New(NewCommandPublisher(rabbitmq.NewPublisher(conn)), redis.NewDedup(testRedis, time.Hour))
	consumer := rabbitmq.NewConsumer(conn, rabbitmq.NotificationsEndpoint.Queues, 1, retry, n.Handler())
	go consumer.Start(ctx)
	return conn
}

func purge(t *testing.T, conn *rabbitmq.Connection) {
	t.Helper()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("channel: %v", err)
	}
	defer func() { _ = ch.Close() }()
	for _, q := range []string{rabbitmq.NotificationsEndpoint.Queues.Main, rabbitmq.CommandsEndpoint.Queues.Main} {
		if _, err := ch.QueuePurge(q, false); err != nil {
			t.Fatalf("purge %s: %v", q, err)
		}
	}
}

// drainCommand waits for one command on the email.delivery queue.
func drainCommand(t *testing.T, conn *rabbitmq.Connection, timeout time.Duration) (mq.DeliveryMessage, bool) {
	t.Helper()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("channel: %v", err)
	}
	defer func() { _ = ch.Close() }()

	dels, err := ch.Consume(rabbitmq.CommandsEndpoint.Queues.Main, "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	select {
	case d := <-dels:
		var cmd mq.DeliveryMessage
		if err := json.Unmarshal(d.Body, &cmd); err != nil {
			t.Fatalf("unmarshal command: %v", err)
		}
		return cmd, true
	case <-time.After(timeout):
		return mq.DeliveryMessage{}, false
	}
}

func TestNotifier_EventBecomesCommand(t *testing.T) {
	ctx := t.Context()
	conn := startNotifier(ctx, t)
	defer func() { _ = conn.Close() }()

	env, err := events.NewReleaseDetected(events.ReleaseDetected{Email: "u@example.com", Repo: "owner/repo", ReleaseTag: "v2.0.0"})
	if err != nil {
		t.Fatalf("build envelope: %v", err)
	}
	if err := rabbitmq.NewPublisher(conn).Publish(ctx, rabbitmq.EventsExchange, string(env.Type), env); err != nil {
		t.Fatalf("publish event: %v", err)
	}

	cmd, ok := drainCommand(t, conn, 20*time.Second)
	if !ok {
		t.Fatal("no command produced from event")
	}
	want := mq.DeliveryMessage{Event: mq.EventNewRelease, Email: "u@example.com", Repo: "owner/repo", Release: "v2.0.0"}
	if cmd.Event != want.Event || cmd.Email != want.Email || cmd.Repo != want.Repo || cmd.Release != want.Release {
		t.Errorf("command = %+v, want %+v", cmd, want)
	}
}

func TestNotifier_DuplicateEventProducesOneCommand(t *testing.T) {
	ctx := t.Context()
	conn := startNotifier(ctx, t)
	defer func() { _ = conn.Close() }()

	env, err := events.NewReleaseDetected(events.ReleaseDetected{Email: "u@example.com", Repo: "owner/repo", ReleaseTag: "v2.0.0"})
	if err != nil {
		t.Fatalf("build envelope: %v", err)
	}

	pub := rabbitmq.NewPublisher(conn)
	for range 2 { // same envelope ID published twice
		if err := pub.Publish(ctx, rabbitmq.EventsExchange, string(env.Type), env); err != nil {
			t.Fatalf("publish event: %v", err)
		}
	}

	if _, ok := drainCommand(t, conn, 20*time.Second); !ok {
		t.Fatal("expected at least one command")
	}
	if _, ok := drainCommand(t, conn, 5*time.Second); ok {
		t.Error("duplicate event produced a second command")
	}
}