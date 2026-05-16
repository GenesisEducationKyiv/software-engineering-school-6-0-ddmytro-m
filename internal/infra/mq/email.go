// Package mq provides message queue functionality for the application.
package mq

import (
	"context"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/api/github"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/db"
)

// DeliveryStream is the Redis stream used for sending email delivery messages.
const DeliveryStream = "messages:delivery"

// EventType represents the type of event in a delivery message.
type EventType string

// Event types for email delivery.
const (
	EventNewRelease        EventType = "new_release"
	EventRepoMoved         EventType = "repo_moved"
	EventEmailVerification EventType = "email_verification"
)

// DeliveryMessage represents a message sent to the delivery stream.
type DeliveryMessage struct {
	Event   EventType      `json:"event"`
	Email   string         `json:"email"`
	Repo    string         `json:"repo,omitempty"`
	Release string         `json:"release,omitempty"`
	Payload map[string]any `json:"payload,omitempty"`
}

// Publisher defines the contract for publishing messages.
type Publisher interface {
	Publish(ctx context.Context, msg any) error
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
	return mq.publisher.Publish(context.Background(), DeliveryMessage{
		Event:   EventNewRelease,
		Email:   sub.Email,
		Repo:    repo.Owner + "/" + repo.Name,
		Release: release.TagName,
	})
}

// SendRepoMoved sends an email notification about a moved or renamed repository.
func (mq *EmailMQ) SendRepoMoved(sub *db.Subscription, repo *db.Repository) error {
	return mq.publisher.Publish(context.Background(), DeliveryMessage{
		Event: EventRepoMoved,
		Email: sub.Email,
		Repo:  repo.Owner + "/" + repo.Name,
	})
}

// SendEmailVerification sends an email verification link to a user.
func (mq *EmailMQ) SendEmailVerification(email, token string) error {
	return mq.publisher.Publish(context.Background(), DeliveryMessage{
		Event: EventEmailVerification,
		Email: email,
		Payload: map[string]any{
			"token": token,
		},
	})
}
