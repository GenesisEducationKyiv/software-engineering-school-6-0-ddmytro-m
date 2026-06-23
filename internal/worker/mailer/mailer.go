// Package mailer consumes email delivery commands and sends them via SMTP.
package mailer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/mq"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/rabbitmq"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/logger"
)

const maxSMTPRetries = 5

// maxResultReportRetries bounds retries for publishing a verification
// outcome. Kept small since, unlike SMTP delivery, the side effect (or its
// failure) has already happened by this point - this is just closing the
// window on reporting it.
const maxResultReportRetries = 3

// ErrUnknownEmailType marks a poison command
var ErrUnknownEmailType = errors.New("unknown email type")

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

// Deliver builds and sends the email for one command, reporting verification
// saga outcomes as a side effect. It is transport-agnostic: both the AMQP
// consumer and the gRPC server call it. A poison command returns
// ErrUnknownEmailType; any other non-nil error is a retryable send failure.
func (m *Mailer) Deliver(ctx context.Context, msg mq.DeliveryMessage) error {
	subject, text, known := buildEmail(msg)
	if !known {
		return fmt.Errorf("%w: %q", ErrUnknownEmailType, msg.Event)
	}

	isVerification := msg.Event == mq.EventEmailVerification

	if err := m.sendWithRetry(ctx, msg.Email, subject, text); err != nil {
		if isVerification && m.results != nil {
			m.reportFailed(ctx, msg, err)
		}
		return err
	}

	if isVerification && m.results != nil {
		m.reportDelivered(ctx, msg)
	}

	return nil
}

// process handles one command and settles it with the broker.
func (m *Mailer) process(ctx context.Context, body []byte, s rabbitmq.Settler) {
	var msg mq.DeliveryMessage
	if err := json.Unmarshal(body, &msg); err != nil {
		s.DeadLetter(ctx, "unmarshal command: "+err.Error())
		return
	}

	err := m.Deliver(ctx, msg)
	switch {
	case err == nil:
		s.Ack()
	case errors.Is(err, ErrUnknownEmailType):
		s.DeadLetter(ctx, err.Error())
	case msg.Event == mq.EventEmailVerification && m.results != nil:
		// Terminal for the saga: the failure was already reported, so ack
		// instead of handing back to broker retries where the signal is lost.
		s.Ack()
	default:
		// In-process retries exhausted; hand back to the broker to retry later.
		s.Retry(ctx, "smtp send failed: "+err.Error())
	}
}

// verificationToken extracts the saga correlation token from a command payload.
func verificationToken(msg mq.DeliveryMessage) string {
	tok, _ := msg.Payload["token"].(string)
	return tok
}

// reportDelivered publishes a verification.delivered result, retrying a
// bounded number of times. If it still fails, the outcome is only logged: the
// command is acked either way since the email was already sent, so retrying
// the whole command would risk sending a duplicate.
func (m *Mailer) reportDelivered(ctx context.Context, msg mq.DeliveryMessage) {
	token := verificationToken(msg)
	if err := m.reportWithRetry(ctx, func() error {
		return m.results.VerificationDelivered(ctx, token)
	}); err != nil {
		logger.Log.Error("failed to publish verification.delivered result after retries",
			zap.Int("attempts", maxResultReportRetries), zap.Error(err))
	}
}

// reportFailed publishes a verification.failed result so the orchestrator can
// compensate, retrying a bounded number of times before giving up and logging.
func (m *Mailer) reportFailed(ctx context.Context, msg mq.DeliveryMessage, cause error) {
	token := verificationToken(msg)
	if err := m.reportWithRetry(ctx, func() error {
		return m.results.VerificationFailed(ctx, token, cause.Error())
	}); err != nil {
		logger.Log.Error("failed to publish verification.failed result after retries",
			zap.Int("attempts", maxResultReportRetries), zap.Error(err))
	}
}

// reportWithRetry retries fn with the mailer's exponential backoff, bounded
// by maxResultReportRetries.
func (m *Mailer) reportWithRetry(ctx context.Context, fn func() error) error {
	backoff := m.baseBackoff
	var err error

	for attempt := range maxResultReportRetries {
		if err = fn(); err == nil {
			return nil
		}

		if attempt < maxResultReportRetries-1 {
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
