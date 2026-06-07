//go:build unit

package mailer

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/mq"
)

type mockStream struct {
	acked       []string
	deadLetters []any
	errDL       error
}

func (m *mockStream) Ack(ctx context.Context, group string, ids ...string) error {
	m.acked = append(m.acked, ids...)
	return nil
}

func (m *mockStream) AutoClaim(ctx context.Context, group, consumer string, minIdle time.Duration, start string, count int64) (msgs []Message, next string, err error) {
	return nil, "", nil
}

func (m *mockStream) EnsureConsumerGroup(ctx context.Context, group string) error {
	return nil
}

func (m *mockStream) PublishDeadLetter(ctx context.Context, msg any) error {
	if m.errDL != nil {
		return m.errDL
	}
	m.deadLetters = append(m.deadLetters, msg)
	return nil
}

func (m *mockStream) ReadGroup(ctx context.Context, group, consumer string, count int64, block time.Duration) ([]Message, error) {
	return nil, nil
}

type mockMessage struct {
	id      string
	payload []byte
}

func (m mockMessage) ID() string      { return m.id }
func (m mockMessage) Payload() []byte { return m.payload }

func TestBuildEmail(t *testing.T) {
	m := &Mailer[mockMessage]{}

	tests := []struct {
		name        string
		msg         mq.DeliveryMessage
		wantSubject string
		wantBody    string
		wantKnown   bool
	}{
		{
			name: "NewRelease",
			msg: mq.DeliveryMessage{
				Event:   mq.EventNewRelease,
				Repo:    "owner/repo",
				Release: "v1.0.0",
			},
			wantSubject: "New release for owner/repo: v1.0.0",
			wantBody:    "A new release v1.0.0 is available for owner/repo.",
			wantKnown:   true,
		},
		{
			name: "RepoMoved",
			msg: mq.DeliveryMessage{
				Event: mq.EventRepoMoved,
				Repo:  "owner/repo",
			},
			wantSubject: "Repository moved: owner/repo",
			wantBody:    "The repository owner/repo has been moved or renamed.",
			wantKnown:   true,
		},
		{
			name: "EmailVerification",
			msg: mq.DeliveryMessage{
				Event: mq.EventEmailVerification,
				Payload: map[string]any{
					"token": "secret-token",
				},
			},
			wantSubject: "Verify your email",
			wantBody:    "Your verification token is secret-token",
			wantKnown:   true,
		},
		{
			name: "UnknownEvent",
			msg: mq.DeliveryMessage{
				Event: "unknown_event_type",
			},
			wantSubject: "",
			wantBody:    "",
			wantKnown:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			subject, body, known := m.buildEmail(tt.msg)
			if subject != tt.wantSubject {
				t.Errorf("subject = %q, want %q", subject, tt.wantSubject)
			}
			if body != tt.wantBody {
				t.Errorf("body = %q, want %q", body, tt.wantBody)
			}
			if known != tt.wantKnown {
				t.Errorf("known = %v, want %v", known, tt.wantKnown)
			}
		})
	}
}

func TestProcessMessage_EmptyPayload(t *testing.T) {
	stream := &mockStream{}
	mailer := NewMailer(stream, "test_group", 1, nil)
	msg := mockMessage{id: "1-0", payload: []byte{}}

	mailer.processMessage(context.Background(), 1, msg)

	if len(stream.deadLetters) != 1 {
		t.Fatalf("expected 1 dead letter, got %d", len(stream.deadLetters))
	}
	if len(stream.acked) != 1 || stream.acked[0] != "1-0" {
		t.Errorf("expected msg to be acked, got %v", stream.acked)
	}
}

func TestProcessMessage_MalformedJSON(t *testing.T) {
	stream := &mockStream{}
	mailer := NewMailer(stream, "test_group", 1, nil)
	msg := mockMessage{id: "1-0", payload: []byte("invalid json")}

	mailer.processMessage(context.Background(), 1, msg)

	if len(stream.deadLetters) != 1 {
		t.Fatalf("expected 1 dead letter, got %d", len(stream.deadLetters))
	}
	if len(stream.acked) != 1 || stream.acked[0] != "1-0" {
		t.Errorf("expected msg to be acked, got %v", stream.acked)
	}
}

func TestProcessMessage_UnknownEvent(t *testing.T) {
	stream := &mockStream{}
	mailer := NewMailer(stream, "test_group", 1, nil)

	validJSON, _ := json.Marshal(mq.DeliveryMessage{Event: "unknown_event_type"})
	msg := mockMessage{id: "1-0", payload: validJSON}

	mailer.processMessage(context.Background(), 1, msg)

	if len(stream.deadLetters) != 1 {
		t.Fatalf("expected 1 dead letter, got %d", len(stream.deadLetters))
	}
	if len(stream.acked) != 1 || stream.acked[0] != "1-0" {
		t.Errorf("expected msg to be acked, got %v", stream.acked)
	}
}

func TestDeadLetter_PublishFailureNotAcking(t *testing.T) {
	stream := &mockStream{errDL: errors.New("publish error")}
	mailer := NewMailer(stream, "test_group", 1, nil)
	msg := mockMessage{id: "1-0", payload: []byte("data")}

	mailer.deadLetter(context.Background(), 1, msg, "test failure")

	if len(stream.acked) != 0 {
		t.Errorf("expected msg NOT to be acked on DL publish failure, got %v", stream.acked)
	}
}

func TestDeadLetter_PublishSuccessAcking(t *testing.T) {
	stream := &mockStream{}
	mailer := NewMailer(stream, "test_group", 1, nil)
	msg := mockMessage{id: "1-0", payload: []byte("data")}

	mailer.deadLetter(context.Background(), 1, msg, "test success")

	if len(stream.deadLetters) != 1 {
		t.Fatalf("expected 1 dead letter, got %d", len(stream.deadLetters))
	}

	dlPayload, ok := stream.deadLetters[0].(map[string]any)
	if !ok {
		t.Fatalf("expected dead letter payload to be map[string]any, got %T", stream.deadLetters[0])
	}
	if dlPayload["original_id"] != "1-0" {
		t.Errorf("expected original_id = 1-0, got %v", dlPayload["original_id"])
	}
	if dlPayload["reason"] != "test success" {
		t.Errorf("expected reason = test success, got %v", dlPayload["reason"])
	}
	if !reflect.DeepEqual(dlPayload["payload"], []byte("data")) {
		t.Errorf("expected payload = data, got %v", dlPayload["payload"])
	}

	if len(stream.acked) != 1 || stream.acked[0] != "1-0" {
		t.Errorf("expected msg to be acked, got %v", stream.acked)
	}
}
