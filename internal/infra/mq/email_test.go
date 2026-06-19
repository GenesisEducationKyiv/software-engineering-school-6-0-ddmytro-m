//go:build unit

package mq

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/api/github"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/contract"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/db"
)

type mockPublisher struct {
	published []any
	err       error
}

func (m *mockPublisher) Publish(ctx context.Context, msg contract.DeliveryMessage) error {
	m.published = append(m.published, msg)
	return m.err
}

func TestEmailMQ_SendNewRelease(t *testing.T) {
	pub := &mockPublisher{}
	mq := NewEmailMQ(pub)

	sub := &db.Subscription{Email: "test@example.com"}
	repo := &db.Repository{Owner: "owner", Name: "repo"}
	release := &github.LatestRelease{TagName: "v1.0.0"}

	err := mq.SendNewRelease(sub, repo, release)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(pub.published) != 1 {
		t.Fatalf("expected 1 message published, got %d", len(pub.published))
	}

	msg, ok := pub.published[0].(contract.DeliveryMessage)
	if !ok {
		t.Fatalf("expected message to be of type contract.DeliveryMessage")
	}

	expected := contract.DeliveryMessage{
		Event:   contract.EventNewRelease,
		Email:   "test@example.com",
		Repo:    "owner/repo",
		Release: "v1.0.0",
	}

	if !reflect.DeepEqual(msg, expected) {
		t.Errorf("expected %+v, got %+v", expected, msg)
	}
}

func TestEmailMQ_SendRepoMoved(t *testing.T) {
	pub := &mockPublisher{}
	mq := NewEmailMQ(pub)

	sub := &db.Subscription{Email: "test@example.com"}
	repo := &db.Repository{Owner: "owner", Name: "repo"}

	err := mq.SendRepoMoved(sub, repo)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(pub.published) != 1 {
		t.Fatalf("expected 1 message published, got %d", len(pub.published))
	}

	msg, ok := pub.published[0].(contract.DeliveryMessage)
	if !ok {
		t.Fatalf("expected message to be of type contract.DeliveryMessage")
	}

	expected := contract.DeliveryMessage{
		Event: contract.EventRepoMoved,
		Email: "test@example.com",
		Repo:  "owner/repo",
	}

	if !reflect.DeepEqual(msg, expected) {
		t.Errorf("expected %+v, got %+v", expected, msg)
	}
}

func TestEmailMQ_SendEmailVerification(t *testing.T) {
	pub := &mockPublisher{}
	mq := NewEmailMQ(pub)

	err := mq.SendEmailVerification("test@example.com", "token123")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(pub.published) != 1 {
		t.Fatalf("expected 1 message published, got %d", len(pub.published))
	}

	msg, ok := pub.published[0].(contract.DeliveryMessage)
	if !ok {
		t.Fatalf("expected message to be of type contract.DeliveryMessage")
	}

	expected := contract.DeliveryMessage{
		Event: contract.EventEmailVerification,
		Email: "test@example.com",
		Payload: map[string]any{
			"token": "token123",
		},
	}

	if !reflect.DeepEqual(msg, expected) {
		t.Errorf("expected %+v, got %+v", expected, msg)
	}
}

func TestEmailMQ_PublishError(t *testing.T) {
	expectedErr := errors.New("publish error")
	pub := &mockPublisher{err: expectedErr}
	mq := NewEmailMQ(pub)

	sub := &db.Subscription{Email: "test@example.com"}
	repo := &db.Repository{Owner: "owner", Name: "repo"}

	err := mq.SendRepoMoved(sub, repo)
	if !errors.Is(err, expectedErr) {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}
}
