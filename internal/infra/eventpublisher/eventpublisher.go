// Package eventpublisher publishes domain events to the broker, satisfying the
// scanner Notifier and HTTP EmailSender interfaces.
package eventpublisher

import (
	"context"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/api/github"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/events"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/db"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/rabbitmq"
)

// Broker publishes a message to an exchange with a routing key.
type Broker interface {
	Publish(ctx context.Context, exchange, routingKey string, msg any) error
}

// Publisher turns domain notifications into published events.
type Publisher struct {
	broker Broker
}

// New creates a Publisher backed by the given broker.
func New(broker Broker) *Publisher {
	return &Publisher{broker: broker}
}

// SendNewRelease publishes a release.detected event.
func (p *Publisher) SendNewRelease(sub *db.Subscription, repo *db.Repository, release *github.LatestRelease) error {
	return p.publish(events.NewReleaseDetected(events.ReleaseDetected{
		Email:      sub.Email,
		Repo:       repo.Owner + "/" + repo.Name,
		ReleaseTag: release.TagName,
	}))
}

// SendRepoMoved publishes a repository.moved event.
func (p *Publisher) SendRepoMoved(sub *db.Subscription, repo *db.Repository) error {
	return p.publish(events.NewRepositoryMoved(events.RepositoryMoved{
		Email: sub.Email,
		Repo:  repo.Owner + "/" + repo.Name,
	}))
}

// SendEmailVerification publishes a subscription.created event.
func (p *Publisher) SendEmailVerification(email, token string) error {
	return p.publish(events.NewSubscriptionCreated(events.SubscriptionCreated{
		Email: email,
		Token: token,
	}))
}

func (p *Publisher) publish(env events.Envelope, err error) error {
	if err != nil {
		return err
	}
	return p.broker.Publish(context.Background(), rabbitmq.EventsExchange, string(env.Type), env)
}
