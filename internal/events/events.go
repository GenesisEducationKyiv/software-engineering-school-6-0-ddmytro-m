// Package events defines the domain event contracts published to the broker.
package events

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Type identifies a domain event; its value equals the broker routing key.
type Type string

// Domain event types. Must match the rabbitmq RoutingKey* constants.
const (
	TypeReleaseDetected     Type = "release.detected"
	TypeRepositoryMoved     Type = "repository.moved"
	TypeSubscriptionCreated Type = "subscription.created"
)

// Version is the current envelope schema version.
const Version = 1

// Envelope wraps a domain event with metadata for routing, dedup, and versioning.
type Envelope struct {
	ID         string          `json:"id"`
	Type       Type            `json:"type"`
	OccurredAt time.Time       `json:"occurred_at"`
	Version    int             `json:"version"`
	Payload    json.RawMessage `json:"payload"`
}

// ReleaseDetected is a new release found on a subscribed repo.
type ReleaseDetected struct {
	Email      string `json:"email"`
	Repo       string `json:"repo"` // owner/name
	ReleaseTag string `json:"release_tag"`
}

// RepositoryMoved is a repository that was moved or renamed.
type RepositoryMoved struct {
	Email string `json:"email"`
	Repo  string `json:"repo"` // owner/name
}

// SubscriptionCreated is a new subscription awaiting verification.
type SubscriptionCreated struct {
	Email string `json:"email"`
	Token string `json:"token"`
}

func newEnvelope(t Type, payload any) (Envelope, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return Envelope{}, fmt.Errorf("marshal %s payload: %w", t, err)
	}
	return Envelope{
		ID:         uuid.NewString(),
		Type:       t,
		OccurredAt: time.Now().UTC(),
		Version:    Version,
		Payload:    body,
	}, nil
}

// NewReleaseDetected builds a release.detected envelope.
func NewReleaseDetected(p ReleaseDetected) (Envelope, error) {
	return newEnvelope(TypeReleaseDetected, p)
}

// NewRepositoryMoved builds a repository.moved envelope.
func NewRepositoryMoved(p RepositoryMoved) (Envelope, error) {
	return newEnvelope(TypeRepositoryMoved, p)
}

// NewSubscriptionCreated builds a subscription.created envelope.
func NewSubscriptionCreated(p SubscriptionCreated) (Envelope, error) {
	return newEnvelope(TypeSubscriptionCreated, p)
}

// DecodeReleaseDetected reads the payload as a ReleaseDetected.
func (e Envelope) DecodeReleaseDetected() (ReleaseDetected, error) {
	var p ReleaseDetected
	err := json.Unmarshal(e.Payload, &p)
	return p, err
}

// DecodeRepositoryMoved reads the payload as a RepositoryMoved.
func (e Envelope) DecodeRepositoryMoved() (RepositoryMoved, error) {
	var p RepositoryMoved
	err := json.Unmarshal(e.Payload, &p)
	return p, err
}

// DecodeSubscriptionCreated reads the payload as a SubscriptionCreated.
func (e Envelope) DecodeSubscriptionCreated() (SubscriptionCreated, error) {
	var p SubscriptionCreated
	err := json.Unmarshal(e.Payload, &p)
	return p, err
}
