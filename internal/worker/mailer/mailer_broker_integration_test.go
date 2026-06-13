//go:build integration

package mailer

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.uber.org/zap"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/mq"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/rabbitmq"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/logger"
)

var testAMQPURL string

func TestMain(m *testing.M) {
	logger.Log = zap.NewNop()

	ctx := context.Background()
	container, url, err := setupRabbitContainer(ctx)
	if err != nil {
		log.Fatalf("failed to start rabbitmq container: %v", err)
	}
	testAMQPURL = url

	code := m.Run()

	if err := container.Terminate(context.Background()); err != nil {
		log.Fatalf("container termination failure: %v", err)
	}
	os.Exit(code)
}

func setupRabbitContainer(ctx context.Context) (testcontainers.Container, string, error) {
	req := testcontainers.ContainerRequest{
		Image:        "rabbitmq:3-management-alpine",
		ExposedPorts: []string{"5672/tcp"},
		WaitingFor:   wait.ForLog("Server startup complete").WithStartupTimeout(120 * time.Second),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
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

type capturedEmail struct {
	to, subject, body string
}

type recordingSender struct {
	done chan capturedEmail
}

func (s *recordingSender) SendEmail(_ context.Context, to, subject, body string) error {
	s.done <- capturedEmail{to, subject, body}
	return nil
}

// startMailer dials a fresh connection, purges the command queues, and starts a
// mailer consumer. It returns the connection and a publisher for the test.
func startMailer(ctx context.Context, t *testing.T, sender EmailSender) (*rabbitmq.Connection, *rabbitmq.Publisher) {
	t.Helper()
	retry := rabbitmq.NewRetryPolicy(time.Second, 2, 3)

	conn, err := rabbitmq.Dial(testAMQPURL, retry)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	purgeQueues(t, conn)

	consumer := rabbitmq.NewConsumer(conn, rabbitmq.CommandsEndpoint.Queues, 1, retry, New(sender).Handler())
	go consumer.Start(ctx)

	return conn, rabbitmq.NewPublisher(conn)
}

func purgeQueues(t *testing.T, conn *rabbitmq.Connection) {
	t.Helper()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("channel: %v", err)
	}
	defer func() { _ = ch.Close() }()
	for _, q := range []string{rabbitmq.CommandsEndpoint.Queues.Main, rabbitmq.CommandsEndpoint.Queues.DLQ} {
		if _, err := ch.QueuePurge(q, false); err != nil {
			t.Fatalf("purge %s: %v", q, err)
		}
	}
}

func TestMailer_ConsumesCommand_SendsEmail(t *testing.T) {
	ctx := t.Context()

	sender := &recordingSender{done: make(chan capturedEmail, 1)}
	conn, pub := startMailer(ctx, t, sender)
	defer func() { _ = conn.Close() }()

	cmd := mq.DeliveryMessage{Event: mq.EventNewRelease, Email: "u@example.com", Repo: "owner/repo", Release: "v9.9.9"}
	if err := pub.Publish(ctx, rabbitmq.CommandsExchange, rabbitmq.RoutingKeyEmailSend, cmd); err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case got := <-sender.done:
		if got.to != cmd.Email {
			t.Errorf("recipient = %q, want %q", got.to, cmd.Email)
		}
		if !strings.Contains(got.body, "v9.9.9") {
			t.Errorf("body %q missing release tag", got.body)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("timed out waiting for the mailer to send the email")
	}
}

func TestMailer_PoisonCommand_DeadLetters(t *testing.T) {
	ctx := t.Context()

	// Sender that would fail the test if ever called (poison must not be sent).
	sender := &recordingSender{done: make(chan capturedEmail, 1)}
	conn, pub := startMailer(ctx, t, sender)
	defer func() { _ = conn.Close() }()

	cmd := mq.DeliveryMessage{Event: "totally-unknown", Email: "u@example.com"}
	if err := pub.Publish(ctx, rabbitmq.CommandsExchange, rabbitmq.RoutingKeyEmailSend, cmd); err != nil {
		t.Fatalf("publish: %v", err)
	}

	if !waitForQueue(ctx, t, conn, rabbitmq.CommandsEndpoint.Queues.DLQ, 1, 20*time.Second) {
		t.Fatal("poison command did not reach the dead-letter queue")
	}
	select {
	case <-sender.done:
		t.Fatal("poison command should not have been sent")
	default:
	}
}

// waitForQueue polls until the named queue holds at least want messages.
func waitForQueue(ctx context.Context, t *testing.T, conn *rabbitmq.Connection, queue string, want int, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ch, err := conn.Channel()
		if err != nil {
			t.Fatalf("channel: %v", err)
		}
		q, err := ch.QueueDeclarePassive(queue, true, false, false, false, nil)
		_ = ch.Close()
		if err == nil && q.Messages >= want {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(300 * time.Millisecond):
		}
	}
	return false
}
