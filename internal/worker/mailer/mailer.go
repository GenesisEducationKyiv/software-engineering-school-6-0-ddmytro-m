// Package mailer provides a worker that consumes delivery messages and sends emails via SMTP.
package mailer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/mq"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/smtp"
)

const (
	stalePendingThreshold = 5 * time.Minute
	maxRetries            = 5
)

// Message represents a single message retrieved from the message queue.
type Message interface {
	// ID returns the unique identifier of the message.
	ID() string
	// Payload returns the raw byte content of the message.
	Payload() []byte
}

// Stream defines the interface for interacting with a message stream.
type Stream[M Message] interface {
	// Ack acknowledges that one or more messages have been processed.
	Ack(ctx context.Context, group string, ids ...string) error
	// AutoClaim transfers ownership of pending messages that have been idle for a specific duration.
	AutoClaim(ctx context.Context, group, consumer string, minIdle time.Duration, start string, count int64) (msgs []M, next string, err error)
	// EnsureConsumerGroup creates the consumer group if it does not already exist.
	EnsureConsumerGroup(ctx context.Context, group string) error
	// PublishDeadLetter moves a failed message to a dead-letter queue for manual inspection.
	PublishDeadLetter(ctx context.Context, msg any) error
	// ReadGroup reads new messages from the stream for a specific consumer group.
	ReadGroup(ctx context.Context, group, consumer string, count int64, block time.Duration) ([]M, error)
}

// Mailer consumes messages from a Redis stream and sends emails.
type Mailer[M Message] struct {
	stream      Stream[M]
	group       string
	workerCount int
	msgQueue    chan M
	smtpClient  *smtp.Client
}

// NewMailer creates a new Mailer instance.
func NewMailer[M Message](stream Stream[M], group string, workerCount int, smtpClient *smtp.Client) *Mailer[M] {
	return &Mailer[M]{
		stream:      stream,
		group:       group,
		workerCount: workerCount,
		msgQueue:    make(chan M, workerCount*2),
		smtpClient:  smtpClient,
	}
}

// Start begins the mailer, starting workers and consuming messages from the stream.
func (m *Mailer[M]) Start(ctx context.Context) {
	if err := m.stream.EnsureConsumerGroup(ctx, m.group); err != nil {
		log.Printf("mailer failed to ensure consumer group: %v", err)
	}

	hostname, _ := os.Hostname()
	consumerName := fmt.Sprintf("%s-%d", hostname, os.Getpid())
	log.Printf("mailer consumer %s started", consumerName)

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
			log.Println("mailer shutting down, waiting for workers...")
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
func (m *Mailer[M]) reclaimStalePending(ctx context.Context, consumerName string) {
	start := "0-0"
	claimed := 0

	for {
		msgs, next, err := m.stream.AutoClaim(ctx, m.group, consumerName, stalePendingThreshold, start, 100)
		if err != nil {
			log.Printf("mailer: failed to autoclaim pending messages: %v", err)
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
		log.Printf("mailer consumer %s: reclaimed %d stale pending messages", consumerName, claimed)
	}
}

func (m *Mailer[M]) consume(ctx context.Context, consumerName string) {
	messages, err := m.stream.ReadGroup(ctx, m.group, consumerName, 10, 2*time.Second)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return
		}
		log.Printf("mailer consumer: error reading from stream: %v", err)
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

func (m *Mailer[M]) worker(ctx context.Context, id int) {
	log.Printf("mailer worker %d started", id)
	for {
		select {
		case <-ctx.Done():
			log.Printf("mailer worker %d shutting down", id)
			return
		case message, ok := <-m.msgQueue:
			if !ok {
				log.Printf("mailer worker %d shutting down", id)
				return
			}
			m.processMessage(ctx, id, message)
		}
	}
}

func (m *Mailer[M]) processMessage(ctx context.Context, workerID int, message M) {
	payload := message.Payload()
	if len(payload) == 0 {
		log.Printf("mailer worker %d: invalid payload format for message %s", workerID, message.ID())
		m.deadLetter(ctx, workerID, message, "invalid payload format")
		return
	}

	var msg mq.DeliveryMessage
	if err := json.Unmarshal(payload, &msg); err != nil {
		log.Printf("mailer worker %d: failed to unmarshal payload %s: %v", workerID, message.ID(), err)
		m.deadLetter(ctx, workerID, message, fmt.Sprintf("unmarshal error: %v", err))
		return
	}

	log.Printf("mailer worker %d: processing event %s for %s", workerID, msg.Event, msg.Email)

	subject, body, known := m.buildEmail(msg)
	if !known {
		// Unknown event — acking silently would lose the message, dead-letter instead.
		log.Printf("mailer worker %d: unknown event type %q for message %s", workerID, msg.Event, message.ID())
		m.deadLetter(ctx, workerID, message, fmt.Sprintf("unknown event type: %q", msg.Event))
		return
	}

	if m.smtpClient == nil {
		log.Printf("mailer worker %d: smtp client is nil, cannot send message %s", workerID, message.ID())
		m.deadLetter(ctx, workerID, message, "smtp client not configured")
		return
	}

	if err := m.sendWithRetry(ctx, workerID, msg.Email, subject, body); err != nil {
		// All retries exhausted. Leave the message un-acked so XAUTOCLAIM
		// can reassign it after the idle threshold, or dead-letter explicitly.
		log.Printf("mailer worker %d: all retries exhausted for message %s, moving to dead-letter", workerID, message.ID())
		m.deadLetter(ctx, workerID, message, fmt.Sprintf("smtp retries exhausted: %v", err))
		return
	}

	if err := m.stream.Ack(ctx, m.group, message.ID()); err != nil {
		log.Printf("mailer worker %d: failed to ack message %s: %v", workerID, message.ID(), err)
	}
}

// buildEmail returns the subject/body for a known event, and false if the
// event type is unrecognised.
func (m *Mailer[M]) buildEmail(msg mq.DeliveryMessage) (subject, body string, known bool) {
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
func (m *Mailer[M]) sendWithRetry(ctx context.Context, workerID int, email, subject, body string) error {
	backoff := time.Second
	var err error

	for attempt := range maxRetries {
		err = m.smtpClient.SendEmail(ctx, email, subject, body)
		if err == nil {
			log.Printf("mailer worker %d: sent email to %s", workerID, email)
			return nil
		}

		log.Printf("mailer worker %d: failed to send email to %s (attempt %d/%d): %v",
			workerID, email, attempt+1, maxRetries, err)

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
func (m *Mailer[M]) deadLetter(ctx context.Context, workerID int, message M, reason string) {
	dlPayload := map[string]any{
		"original_id": message.ID(),
		"reason":      reason,
		"payload":     message.Payload(),
		"failed_at":   time.Now().UTC().Format(time.RFC3339),
	}

	if err := m.stream.PublishDeadLetter(ctx, dlPayload); err != nil {
		log.Printf("mailer worker %d: failed to publish dead-letter for message %s: %v", workerID, message.ID(), err)
		// Do not ack — leave in PEL so it can be inspected or reclaimed.
		return
	}

	if err := m.stream.Ack(ctx, m.group, message.ID()); err != nil {
		log.Printf("mailer worker %d: failed to ack dead-lettered message %s: %v", workerID, message.ID(), err)
	}
}
