//go:build unit

package mailer

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"

	"go.uber.org/zap"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/mq"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/logger"
)

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

func init() {
	logger.Log = zap.NewNop()
}

type sentEmail struct {
	to, subject, body string
}

type fakeSender struct {
	mu       sync.Mutex
	sent     []sentEmail
	failures int // fail this many initial calls, then succeed
}

func (f *fakeSender) SendEmail(_ context.Context, to, subject, body string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent = append(f.sent, sentEmail{to, subject, body})
	if len(f.sent) <= f.failures {
		return errors.New("smtp boom")
	}
	return nil
}

func (f *fakeSender) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.sent)
}

type fakeSettler struct {
	acked        bool
	retried      string
	deadLettered string
}

func (s *fakeSettler) Ack()                                   { s.acked = true }
func (s *fakeSettler) Retry(_ context.Context, r string)      { s.retried = r }
func (s *fakeSettler) DeadLetter(_ context.Context, r string) { s.deadLettered = r }

func newMailer(sender EmailSender) *Mailer {
	m := New(sender)
	m.baseBackoff = 0 // no waiting between retries in tests
	return m
}

func TestProcess_KnownEvents_SendAndAck(t *testing.T) {
	cases := []struct {
		name        string
		cmd         mq.DeliveryMessage
		wantInBody  string
		wantSubject string
	}{
		{
			name:        "new release",
			cmd:         mq.DeliveryMessage{Event: mq.EventNewRelease, Email: "u@example.com", Repo: "owner/repo", Release: "v1.2.3"},
			wantInBody:  "v1.2.3",
			wantSubject: "New release for owner/repo: v1.2.3",
		},
		{
			name:        "repo moved",
			cmd:         mq.DeliveryMessage{Event: mq.EventRepoMoved, Email: "u@example.com", Repo: "owner/repo"},
			wantInBody:  "moved or renamed",
			wantSubject: "Repository moved: owner/repo",
		},
		{
			name:        "email verification",
			cmd:         mq.DeliveryMessage{Event: mq.EventEmailVerification, Email: "u@example.com", Payload: map[string]any{"token": "tok123"}},
			wantInBody:  "tok123",
			wantSubject: "Verify your email",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sender := &fakeSender{}
			m := newMailer(sender)
			s := &fakeSettler{}

			m.process(context.Background(), mustJSON(t, tc.cmd), s)

			if !s.acked {
				t.Fatalf("expected ack, got retry=%q deadLetter=%q", s.retried, s.deadLettered)
			}
			if sender.count() != 1 {
				t.Fatalf("expected 1 send, got %d", sender.count())
			}
			got := sender.sent[0]
			if got.to != tc.cmd.Email {
				t.Errorf("recipient = %q, want %q", got.to, tc.cmd.Email)
			}
			if got.subject != tc.wantSubject {
				t.Errorf("subject = %q, want %q", got.subject, tc.wantSubject)
			}
			if !strings.Contains(got.body, tc.wantInBody) {
				t.Errorf("body %q missing %q", got.body, tc.wantInBody)
			}
		})
	}
}

func TestProcess_UnknownEvent_DeadLetters(t *testing.T) {
	sender := &fakeSender{}
	m := newMailer(sender)
	s := &fakeSettler{}

	m.process(context.Background(), mustJSON(t, mq.DeliveryMessage{Event: "bogus", Email: "u@example.com"}), s)

	if s.deadLettered == "" {
		t.Fatalf("expected dead-letter, got ack=%v retry=%q", s.acked, s.retried)
	}
	if sender.count() != 0 {
		t.Errorf("expected no send for unknown event, got %d", sender.count())
	}
}

func TestProcess_MalformedJSON_DeadLetters(t *testing.T) {
	sender := &fakeSender{}
	m := newMailer(sender)
	s := &fakeSettler{}

	m.process(context.Background(), []byte("{not json"), s)

	if s.deadLettered == "" {
		t.Fatalf("expected dead-letter, got ack=%v retry=%q", s.acked, s.retried)
	}
	if sender.count() != 0 {
		t.Errorf("expected no send for malformed payload, got %d", sender.count())
	}
}

func TestProcess_SMTPFailureExhausted_Retries(t *testing.T) {
	sender := &fakeSender{failures: 1000} // always fail
	m := newMailer(sender)
	s := &fakeSettler{}

	m.process(context.Background(), mustJSON(t, mq.DeliveryMessage{Event: mq.EventNewRelease, Email: "u@example.com", Repo: "o/r", Release: "v1"}), s)

	if s.retried == "" {
		t.Fatalf("expected retry, got ack=%v deadLetter=%q", s.acked, s.deadLettered)
	}
	if sender.count() != m.maxRetries {
		t.Errorf("expected %d send attempts, got %d", m.maxRetries, sender.count())
	}
}

func TestProcess_SMTPRecovers_Acks(t *testing.T) {
	sender := &fakeSender{failures: 2} // fail twice, succeed on third
	m := newMailer(sender)
	s := &fakeSettler{}

	m.process(context.Background(), mustJSON(t, mq.DeliveryMessage{Event: mq.EventNewRelease, Email: "u@example.com", Repo: "o/r", Release: "v1"}), s)

	if !s.acked {
		t.Fatalf("expected ack after recovery, got retry=%q deadLetter=%q", s.retried, s.deadLettered)
	}
	if sender.count() != 3 {
		t.Errorf("expected 3 send attempts, got %d", sender.count())
	}
}
