// Package notifier consumes domain events and emits email delivery commands.
package notifier

import (
	"context"
	"encoding/json"
	"fmt"

	"go.uber.org/zap"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/events"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/mq"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/logger"
)

const dedupKeyPrefix = "processed:"

// CommandPublisher publishes an email delivery command.
type CommandPublisher interface {
	Publish(ctx context.Context, cmd mq.DeliveryMessage) error
}

// DedupStore records processed event IDs to suppress duplicate deliveries.
type DedupStore interface {
	MarkProcessed(ctx context.Context, key string) (bool, error)
	Unmark(ctx context.Context, key string) error
}

// settler settles a single delivery with the broker.
type settler interface {
	Ack()
	Retry(ctx context.Context, reason string)
	DeadLetter(ctx context.Context, reason string)
}

// Notifier maps domain events to email commands.
type Notifier struct {
	publisher CommandPublisher
	dedup     DedupStore
}

// New creates a Notifier.
func New(publisher CommandPublisher, dedup DedupStore) *Notifier {
	return &Notifier{publisher: publisher, dedup: dedup}
}

// process handles one event message end to end and settles it.
func (n *Notifier) process(ctx context.Context, body []byte, s settler) {
	var env events.Envelope
	if err := json.Unmarshal(body, &env); err != nil {
		s.DeadLetter(ctx, "unmarshal envelope: "+err.Error())
		return
	}

	cmd, send, err := toCommand(env)
	if err != nil {
		s.DeadLetter(ctx, err.Error())
		return
	}
	if !send {
		// Valid event that warrants no command — settle it as handled.
		logger.Log.Debug("event has no command, skipping", zap.String("event_id", env.ID), zap.String("type", string(env.Type)))
		s.Ack()
		return
	}

	key := dedupKeyPrefix + env.ID
	fresh, err := n.dedup.MarkProcessed(ctx, key)
	if err != nil {
		s.Retry(ctx, "dedup check: "+err.Error())
		return
	}
	if !fresh {
		logger.Log.Info("duplicate event skipped", zap.String("event_id", env.ID), zap.String("type", string(env.Type)))
		s.Ack()
		return
	}

	if pubErr := n.publisher.Publish(ctx, cmd); pubErr != nil {
		// Roll back the dedup marker so the redelivery is not skipped.
		if unmarkErr := n.dedup.Unmark(ctx, key); unmarkErr != nil {
			logger.Log.Error("failed to roll back dedup key", zap.String("event_id", env.ID), zap.Error(unmarkErr))
		}
		s.Retry(ctx, "publish command: "+pubErr.Error())
		return
	}

	s.Ack()
}

// toCommand maps an event to a command. send is false when no command is
// warranted (ack); a non-nil error means poison (dead-letter).
func toCommand(env events.Envelope) (cmd mq.DeliveryMessage, send bool, err error) {
	switch env.Type {
	case events.TypeReleaseDetected:
		p, decErr := env.DecodeReleaseDetected()
		if decErr != nil {
			return mq.DeliveryMessage{}, false, fmt.Errorf("decode %s: %w", env.Type, decErr)
		}
		return mq.DeliveryMessage{Event: mq.EventNewRelease, Email: p.Email, Repo: p.Repo, Release: p.ReleaseTag}, true, nil
	case events.TypeRepositoryMoved:
		p, decErr := env.DecodeRepositoryMoved()
		if decErr != nil {
			return mq.DeliveryMessage{}, false, fmt.Errorf("decode %s: %w", env.Type, decErr)
		}
		return mq.DeliveryMessage{Event: mq.EventRepoMoved, Email: p.Email, Repo: p.Repo}, true, nil
	case events.TypeSubscriptionCreated:
		p, decErr := env.DecodeSubscriptionCreated()
		if decErr != nil {
			return mq.DeliveryMessage{}, false, fmt.Errorf("decode %s: %w", env.Type, decErr)
		}
		return mq.DeliveryMessage{Event: mq.EventEmailVerification, Email: p.Email, Payload: map[string]any{"token": p.Token}}, true, nil
	default:
		return mq.DeliveryMessage{}, false, fmt.Errorf("unknown event type: %q", env.Type)
	}
}
