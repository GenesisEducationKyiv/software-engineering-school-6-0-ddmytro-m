//go:build unit

package eventpublisher

import (
	"context"
	"errors"
	"testing"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/api/github"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/events"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/db"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/rabbitmq"
)

type capturedPublish struct {
	exchange   string
	routingKey string
	msg        any
}

type mockBroker struct {
	calls []capturedPublish
	err   error
}

func (m *mockBroker) Publish(_ context.Context, exchange, routingKey string, msg any) error {
	m.calls = append(m.calls, capturedPublish{exchange, routingKey, msg})
	return m.err
}

func lastEnvelope(t *testing.T, b *mockBroker) events.Envelope {
	t.Helper()
	if len(b.calls) != 1 {
		t.Fatalf("expected 1 publish, got %d", len(b.calls))
	}
	call := b.calls[0]
	if call.exchange != rabbitmq.EventsExchange {
		t.Errorf("exchange = %q, want %q", call.exchange, rabbitmq.EventsExchange)
	}
	env, ok := call.msg.(events.Envelope)
	if !ok {
		t.Fatalf("published message is %T, want events.Envelope", call.msg)
	}
	if call.routingKey != string(env.Type) {
		t.Errorf("routing key = %q, want %q (event type)", call.routingKey, env.Type)
	}
	if env.ID == "" {
		t.Error("envelope ID is empty")
	}
	return env
}

func TestSendNewRelease(t *testing.T) {
	b := &mockBroker{}
	p := New(b)

	if err := p.SendNewRelease(
		&db.Subscription{Email: "u@example.com"},
		&db.Repository{Owner: "owner", Name: "repo"},
		&github.LatestRelease{TagName: "v1.0.0"},
	); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	env := lastEnvelope(t, b)
	if env.Type != events.TypeReleaseDetected {
		t.Fatalf("type = %q, want %q", env.Type, events.TypeReleaseDetected)
	}
	got, err := env.DecodeReleaseDetected()
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	want := events.ReleaseDetected{Email: "u@example.com", Repo: "owner/repo", ReleaseTag: "v1.0.0"}
	if got != want {
		t.Errorf("payload = %+v, want %+v", got, want)
	}
}

func TestSendRepoMoved(t *testing.T) {
	b := &mockBroker{}
	p := New(b)

	if err := p.SendRepoMoved(
		&db.Subscription{Email: "u@example.com"},
		&db.Repository{Owner: "owner", Name: "repo"},
	); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	env := lastEnvelope(t, b)
	if env.Type != events.TypeRepositoryMoved {
		t.Fatalf("type = %q, want %q", env.Type, events.TypeRepositoryMoved)
	}
	got, err := env.DecodeRepositoryMoved()
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	want := events.RepositoryMoved{Email: "u@example.com", Repo: "owner/repo"}
	if got != want {
		t.Errorf("payload = %+v, want %+v", got, want)
	}
}

func TestSendEmailVerification(t *testing.T) {
	b := &mockBroker{}
	p := New(b)

	if err := p.SendEmailVerification("u@example.com", "token123"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	env := lastEnvelope(t, b)
	if env.Type != events.TypeSubscriptionCreated {
		t.Fatalf("type = %q, want %q", env.Type, events.TypeSubscriptionCreated)
	}
	got, err := env.DecodeSubscriptionCreated()
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	want := events.SubscriptionCreated{Email: "u@example.com", Token: "token123"}
	if got != want {
		t.Errorf("payload = %+v, want %+v", got, want)
	}
}

func TestPublishErrorPropagates(t *testing.T) {
	wantErr := errors.New("broker down")
	b := &mockBroker{err: wantErr}
	p := New(b)

	err := p.SendRepoMoved(&db.Subscription{Email: "u@example.com"}, &db.Repository{Owner: "o", Name: "r"})
	if !errors.Is(err, wantErr) {
		t.Errorf("error = %v, want %v", err, wantErr)
	}
}