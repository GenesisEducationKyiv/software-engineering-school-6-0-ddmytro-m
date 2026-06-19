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

// ResultPublisher reports the outcome of a verification email to the onboarding
// saga. Both methods are correlated by the non-PII confirm token.
type ResultPublisher interface {
	VerificationDelivered(ctx context.Context, token string) error
	VerificationFailed(ctx context.Context, token, reason string) error
}

// Mailer turns delivery commands into emails.
type Mailer struct {
	sender      EmailSender
	results     ResultPublisher
	maxRetries  int
	baseBackoff time.Duration
}

// New creates a Mailer. results may be nil, in which case verification outcomes
// are not reported and failures fall back to broker-level retries.
func New(sender EmailSender, results ResultPublisher) *Mailer {
	return &Mailer{sender: sender, results: results, maxRetries: maxSMTPRetries, baseBackoff: time.Second}
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

	isVerification := msg.Event == mq.EventEmailVerification

	if err := m.sendWithRetry(ctx, msg.Email, subject, text); err != nil {
		if isVerification && m.results != nil {
			// Terminal for the saga: report the failure and ack, instead of
			// handing back to broker retries where the signal would be lost.
			m.reportFailed(ctx, msg, err)
			s.Ack()
			return
		}
		// In-process retries exhausted; hand back to the broker to retry later.
		s.Retry(ctx, "smtp send failed: "+err.Error())
		return
	}

	if isVerification && m.results != nil {
		m.reportDelivered(ctx, msg)
	}

	s.Ack()
}

// verificationToken extracts the saga correlation token from a command payload.
func verificationToken(msg mq.DeliveryMessage) string {
	tok, _ := msg.Payload["token"].(string)
	return tok
}

// reportDelivered publishes a verification.delivered result; publish failures are
// logged, not retried, since the email was already sent.
func (m *Mailer) reportDelivered(ctx context.Context, msg mq.DeliveryMessage) {
	if err := m.results.VerificationDelivered(ctx, verificationToken(msg)); err != nil {
		logger.Log.Error("failed to publish verification.delivered result", zap.Error(err))
	}
}

// reportFailed publishes a verification.failed result so the orchestrator can
// compensate; publish failures are logged.
func (m *Mailer) reportFailed(ctx context.Context, msg mq.DeliveryMessage, cause error) {
	if err := m.results.VerificationFailed(ctx, verificationToken(msg), cause.Error()); err != nil {
		logger.Log.Error("failed to publish verification.failed result", zap.Error(err))
	}
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
