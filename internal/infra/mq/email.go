// Package mq maps domain types into the delivery contract and publishes them.
// It is server-side only; the mailer depends on internal/contract instead.
package mq

import (
	"context"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/api/github"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/contract"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/db"
)

// Publisher defines the contract for publishing messages.
type Publisher interface {
	Publish(ctx context.Context, msg contract.DeliveryMessage) error
}

// jsonPublisher is an unexported interface that defines the contract for a
// logic-agnostic publisher that serializes messages to JSON, like redis.Stream.
type jsonPublisher interface {
	Publish(ctx context.Context, msg any) error
}

// deliveryPublisherAdapter adapts a jsonPublisher to the typed Publisher interface.
type deliveryPublisherAdapter struct {
	jp jsonPublisher
}

// Publish satisfies the Publisher interface by calling the underlying JSON publisher.
func (a *deliveryPublisherAdapter) Publish(ctx context.Context, msg contract.DeliveryMessage) error {
	return a.jp.Publish(ctx, msg)
}

// NewDeliveryPublisher creates an adapter for the jsonPublisher to
// satisfy the Publisher interface.
func NewDeliveryPublisher(jp jsonPublisher) Publisher {
	return &deliveryPublisherAdapter{jp: jp}
}

// EmailMQ is an implementation of a message queue for sending emails.
type EmailMQ struct {
	publisher Publisher
}

// NewEmailMQ creates a new EmailMQ instance via Dependency Injection.
func NewEmailMQ(publisher Publisher) *EmailMQ {
	return &EmailMQ{
		publisher: publisher,
	}
}

// SendNewRelease sends an email notification about a new release.
func (mq *EmailMQ) SendNewRelease(sub *db.Subscription, repo *db.Repository, release *github.LatestRelease) error {
	return mq.publisher.Publish(context.Background(), contract.DeliveryMessage{
		Event:   contract.EventNewRelease,
		Email:   sub.Email,
		Repo:    repo.Owner + "/" + repo.Name,
		Release: release.TagName,
	})
}

// SendRepoMoved sends an email notification about a moved or renamed repository.
func (mq *EmailMQ) SendRepoMoved(sub *db.Subscription, repo *db.Repository) error {
	return mq.publisher.Publish(context.Background(), contract.DeliveryMessage{
		Event: contract.EventRepoMoved,
		Email: sub.Email,
		Repo:  repo.Owner + "/" + repo.Name,
	})
}

// SendEmailVerification sends an email verification link to a user.
func (mq *EmailMQ) SendEmailVerification(email, token string) error {
	return mq.publisher.Publish(context.Background(), contract.DeliveryMessage{
		Event: contract.EventEmailVerification,
		Email: email,
		Payload: map[string]any{
			"token": token,
		},
	})
}
