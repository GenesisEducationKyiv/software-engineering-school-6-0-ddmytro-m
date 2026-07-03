// Package mailer consumes email delivery commands and sends them via SMTP.
package mailer

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/mq"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/rabbitmq"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/logger"
)

const maxSMTPRetries = 5

// EmailSender sends a single email.
type EmailSender interface {
	SendEmail(ctx context.Context, to, subject, body string) error
}

// Mailer turns delivery commands into emails.
type Mailer struct {
	sender      EmailSender
	maxRetries  int
	baseBackoff time.Duration
}

// New creates a Mailer.
func New(sender EmailSender) *Mailer {
	return &Mailer{sender: sender, maxRetries: maxSMTPRetries, baseBackoff: time.Second}
}

// Handler adapts the mailer to a rabbitmq DeliveryHandler.
func (m *Mailer) Handler() rabbitmq.DeliveryHandler {
	return func(ctx context.Context, body []byte, s rabbitmq.Settler) {
		m.process(ctx, body, s)
	}
}

// process handles one command and settles it.
func (m *Mailer) process(ctx context.Context, body []byte, s rabbitmq.Settler) {
	var msg mq.DeliveryMessage
	if err := json.Unmarshal(body, &msg); err != nil {
		s.DeadLetter(ctx, "unmarshal command: "+err.Error())
		return
	}

	subject, text, known := buildEmail(msg)
	if !known {
		s.DeadLetter(ctx, fmt.Sprintf("unknown event type: %q", msg.Event))
		return
	}

	if err := m.sendWithRetry(ctx, msg.Email, subject, text); err != nil {
		// In-process retries exhausted; hand back to the broker to retry later.
		s.Retry(ctx, "smtp send failed: "+err.Error())
		return
	}

	s.Ack()
}

func buildEmail(msg mq.DeliveryMessage) (subject, body string, known bool) {
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

// sendWithRetry sends an email with in-process exponential backoff.
func (m *Mailer) sendWithRetry(ctx context.Context, email, subject, body string) error {
	backoff := m.baseBackoff
	var err error

	for attempt := range m.maxRetries {
		err = m.sender.SendEmail(ctx, email, subject, body)
		if err == nil {
			logger.Log.Debug("sent email")
			return nil
		}

		logger.Log.Error("failed to send email", zap.Int("attempt", attempt+1), zap.Int("max", m.maxRetries), zap.Error(err))

		if attempt < m.maxRetries-1 {
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
