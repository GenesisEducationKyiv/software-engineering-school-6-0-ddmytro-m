// Package mailer provides a worker that consumes delivery messages and sends emails via SMTP.
package mailer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"go.uber.org/zap"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/mq"
	redisDB "github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/redis"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/smtp"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/logger"
)

const (
	stalePendingThreshold = 5 * time.Minute
	maxRetries            = 5
)

// Mailer consumes messages from a Redis stream and sends emails.
type Mailer struct {
	stream      *redisDB.Stream
	group       string
	workerCount int
	msgQueue    chan redis.XMessage
	smtpClient  *smtp.Client
}

// NewMailer creates a new Mailer instance.
func NewMailer(stream *redisDB.Stream, group string, workerCount int, smtpClient *smtp.Client) *Mailer {
	return &Mailer{
		stream:      stream,
		group:       group,
		workerCount: workerCount,
		msgQueue:    make(chan redis.XMessage, workerCount*2),
		smtpClient:  smtpClient,
	}
}

// Start begins the mailer, starting workers and consuming messages from the stream.
func (m *Mailer) Start(ctx context.Context) {
	if err := m.stream.EnsureConsumerGroup(ctx, m.group); err != nil {
		logger.Log.Error("mailer failed to ensure consumer group", zap.Error(err))
	}

	hostname, _ := os.Hostname()
	consumerName := fmt.Sprintf("%s-%d", hostname, os.Getpid())
	logger.Log.Info("mailer consumer started", zap.String("consumer", consumerName))

	// reclaim any messages that were in-flight when a previous instance crashed
	// and have been pending longer than the idle threshold.
	m.reclaimStalePending(ctx, consumerName)

	var wg sync.WaitGroup
	for i := 0; i < m.workerCount; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			m.worker(ctx, id)
		}(i)
	}

	for {
		if ctx.Err() != nil {
			logger.Log.Info("mailer shutting down, waiting for workers...")
			close(m.msgQueue)
			wg.Wait()
			return
		}
		m.consume(ctx, consumerName)
	}
}

// reclaimStalePending uses XAUTOCLAIM to pull back messages that have been
// sitting in the PEL (i.e. delivered but never acked) for longer than
// stalePendingThreshold. this covers crashes and hard-killed workers.
func (m *Mailer) reclaimStalePending(ctx context.Context, consumerName string) {
	start := "0-0"
	claimed := 0

	for {
		msgs, next, err := m.stream.AutoClaim(ctx, m.group, consumerName, stalePendingThreshold, start, 100)
		if err != nil {
			logger.Log.Error("failed to autoclaim pending messages", zap.Error(err))
			return
		}
		for _, msg := range msgs {
			select {
			case <-ctx.Done():
				return
			case m.msgQueue <- msg:
				claimed++
			}
		}
		if next == "0-0" || next == "" {
			break
		}
		start = next
	}

	if claimed > 0 {
		logger.Log.Info("reclaimed stale pending messages", zap.String("consumer", consumerName), zap.Int("claimed", claimed))
	}
}

func (m *Mailer) consume(ctx context.Context, consumerName string) {
	messages, err := m.stream.ReadGroup(ctx, m.group, consumerName, 10, 2*time.Second)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, redis.Nil) {
			return
		}
		logger.Log.Error("error reading from stream", zap.Error(err))
		select {
		case <-ctx.Done():
		case <-time.After(1 * time.Second):
		}
		return
	}

	for _, message := range messages {
		select {
		case <-ctx.Done():
			return
		case m.msgQueue <- message:
		}
	}
}

func (m *Mailer) worker(ctx context.Context, id int) {
	logger.Log.Info("mailer worker started", zap.Int("worker_id", id))
	for {
		select {
		case <-ctx.Done():
			logger.Log.Info("mailer worker shutting down", zap.Int("worker_id", id))
			return
		case message, ok := <-m.msgQueue:
			if !ok {
				logger.Log.Info("mailer worker shutting down", zap.Int("worker_id", id))
				return
			}
			m.processMessage(ctx, id, message)
		}
	}
}

func (m *Mailer) processMessage(ctx context.Context, workerID int, message redis.XMessage) {
	payloadStr, ok := message.Values["payload"].(string)
	if !ok {
		logger.Log.Error("invalid payload format", zap.Int("worker_id", workerID), zap.String("message_id", message.ID))
		m.deadLetter(ctx, workerID, message, "invalid payload format")
		return
	}

	var msg mq.DeliveryMessage
	if err := json.Unmarshal([]byte(payloadStr), &msg); err != nil {
		logger.Log.Error("failed to unmarshal payload", zap.Int("worker_id", workerID), zap.String("message_id", message.ID), zap.Error(err))
		m.deadLetter(ctx, workerID, message, fmt.Sprintf("unmarshal error: %v", err))
		return
	}

	logger.Log.Info("processing event", zap.Int("worker_id", workerID), zap.String("event", string(msg.Event)), zap.String("email", msg.Email))

	subject, body, known := m.buildEmail(msg)
	if !known {
		// Unknown event — acking silently would lose the message, dead-letter instead.
		logger.Log.Error("unknown event type", zap.Int("worker_id", workerID), zap.String("event", string(msg.Event)), zap.String("message_id", message.ID))
		m.deadLetter(ctx, workerID, message, fmt.Sprintf("unknown event type: %q", msg.Event))
		return
	}

	if m.smtpClient == nil {
		logger.Log.Error("smtp client is nil", zap.Int("worker_id", workerID), zap.String("message_id", message.ID))
		m.deadLetter(ctx, workerID, message, "smtp client not configured")
		return
	}

	if err := m.sendWithRetry(ctx, workerID, msg.Email, subject, body); err != nil {
		// All retries exhausted. Leave the message un-acked so XAUTOCLAIM
		// can reassign it after the idle threshold, or dead-letter explicitly.
		logger.Log.Error("all retries exhausted for message, moving to dead-letter", zap.Int("worker_id", workerID), zap.String("message_id", message.ID))
		m.deadLetter(ctx, workerID, message, fmt.Sprintf("smtp retries exhausted: %v", err))
		return
	}

	if err := m.stream.Ack(ctx, m.group, message.ID); err != nil {
		logger.Log.Error("failed to ack message", zap.Int("worker_id", workerID), zap.String("message_id", message.ID), zap.Error(err))
	}
}

// buildEmail returns the subject/body for a known event, and false if the
// event type is unrecognised.
func (m *Mailer) buildEmail(msg mq.DeliveryMessage) (subject, body string, known bool) {
	switch msg.Event {
	case mq.EventNewRelease:
		return fmt.Sprintf("New release for %s: %s", msg.Repo, msg.Release),
			fmt.Sprintf("A new release %s is available for %s.", msg.Release, msg.Repo),
			true
	case mq.EventRepoMoved:
		return fmt.Sprintf("Repository moved: %s", msg.Repo),
			fmt.Sprintf("The repository %s has been moved or renamed.", msg.Repo),
			true
	case mq.EventEmailVerification:
		return "Verify your email",
			fmt.Sprintf("Your verification token is %v", msg.Payload["token"]),
			true
	default:
		return "", "", false
	}
}

// sendWithRetry attempts to send an email with exponential backoff.
// Returns the last error if all attempts fail.
func (m *Mailer) sendWithRetry(ctx context.Context, workerID int, email, subject, body string) error {
	backoff := time.Second
	var err error

	for attempt := range maxRetries {
		err = m.smtpClient.SendEmail(ctx, email, subject, body)
		if err == nil {
			logger.Log.Info("sent email", zap.Int("worker_id", workerID), zap.String("email", email))
			return nil
		}

		logger.Log.Error("failed to send email", zap.Int("worker_id", workerID), zap.String("email", email), zap.Int("attempt", attempt+1), zap.Int("max_retries", maxRetries), zap.Error(err))

		if attempt < maxRetries-1 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
				backoff *= 2
			}
		}
	}

	return err
}

// deadLetter acks the message out of the main stream and writes it to a
// separate dead-letter stream for later inspection / manual replay.
func (m *Mailer) deadLetter(ctx context.Context, workerID int, message redis.XMessage, reason string) {
	dlPayload := map[string]any{
		"original_id": message.ID,
		"reason":      reason,
		"payload":     message.Values["payload"],
		"failed_at":   time.Now().UTC().Format(time.RFC3339),
	}

	if err := m.stream.PublishDeadLetter(ctx, dlPayload); err != nil {
		logger.Log.Error("failed to publish dead-letter", zap.Int("worker_id", workerID), zap.String("message_id", message.ID), zap.Error(err))
		// Do not ack — leave in PEL so it can be inspected or reclaimed.
		return
	}

	if err := m.stream.Ack(ctx, m.group, message.ID); err != nil {
		logger.Log.Error("failed to ack dead-lettered message", zap.Int("worker_id", workerID), zap.String("message_id", message.ID), zap.Error(err))
	}
}
