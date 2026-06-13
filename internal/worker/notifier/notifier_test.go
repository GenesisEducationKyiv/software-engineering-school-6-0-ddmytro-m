//go:build unit

package notifier

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"go.uber.org/zap"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/events"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/mq"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/logger"
)

func init() {
	logger.Log = zap.NewNop()
}

type fakeCommandPublisher struct {
	cmds []mq.DeliveryMessage
	err  error
}

func (f *fakeCommandPublisher) Publish(_ context.Context, cmd mq.DeliveryMessage) error {
	f.cmds = append(f.cmds, cmd)
	return f.err
}

type fakeDedup struct {
	fresh    bool
	markErr  error
	marked   []string
	unmarked []string
}

func (f *fakeDedup) MarkProcessed(_ context.Context, key string) (bool, error) {
	f.marked = append(f.marked, key)
	if f.markErr != nil {
		return false, f.markErr
	}
	return f.fresh, nil
}

func (f *fakeDedup) Unmark(_ context.Context, key string) error {
	f.unmarked = append(f.unmarked, key)
	return nil
}

type fakeSettler struct {
	acked        bool
	retried      string
	deadLettered string
}

func (s *fakeSettler) Ack()                                   { s.acked = true }
func (s *fakeSettler) Retry(_ context.Context, r string)      { s.retried = r }
func (s *fakeSettler) DeadLetter(_ context.Context, r string) { s.deadLettered = r }

func mustEnvelope(t *testing.T, env events.Envelope, err error) (events.Envelope, []byte) {
	t.Helper()
	if err != nil {
		t.Fatalf("build envelope: %v", err)
	}
	b, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	return env, b
}

func TestProcess_MapsEventsAndAcks(t *testing.T) {
	cases := []struct {
		name string
		env  events.Envelope
		want mq.DeliveryMessage
	}{
		{
			name: "release detected",
			env:  must(events.NewReleaseDetected(events.ReleaseDetected{Email: "u@example.com", Repo: "owner/repo", ReleaseTag: "v1.2.3"})),
			want: mq.DeliveryMessage{Event: mq.EventNewRelease, Email: "u@example.com", Repo: "owner/repo", Release: "v1.2.3"},
		},
		{
			name: "repository moved",
			env:  must(events.NewRepositoryMoved(events.RepositoryMoved{Email: "u@example.com", Repo: "owner/repo"})),
			want: mq.DeliveryMessage{Event: mq.EventRepoMoved, Email: "u@example.com", Repo: "owner/repo"},
		},
		{
			name: "subscription created",
			env:  must(events.NewSubscriptionCreated(events.SubscriptionCreated{Email: "u@example.com", Token: "tok123"})),
			want: mq.DeliveryMessage{Event: mq.EventEmailVerification, Email: "u@example.com", Payload: map[string]any{"token": "tok123"}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pub := &fakeCommandPublisher{}
			dedup := &fakeDedup{fresh: true}
			n := New(pub, dedup)
			s := &fakeSettler{}

			env, body := mustEnvelope(t, tc.env, nil)
			n.process(context.Background(), body, s)

			if !s.acked {
				t.Fatalf("expected ack, retry=%q dl=%q", s.retried, s.deadLettered)
			}
			if len(pub.cmds) != 1 {
				t.Fatalf("expected 1 command, got %d", len(pub.cmds))
			}
			if !reflect.DeepEqual(pub.cmds[0], tc.want) {
				t.Errorf("command = %+v, want %+v", pub.cmds[0], tc.want)
			}
			wantKey := dedupKeyPrefix + env.ID
			if len(dedup.marked) != 1 || dedup.marked[0] != wantKey {
				t.Errorf("marked keys = %v, want [%s]", dedup.marked, wantKey)
			}
		})
	}
}

func TestProcess_DuplicateEvent_AcksWithoutPublishing(t *testing.T) {
	pub := &fakeCommandPublisher{}
	dedup := &fakeDedup{fresh: false} // already seen
	n := New(pub, dedup)
	s := &fakeSettler{}

	_, body := mustEnvelope(t, must(events.NewReleaseDetected(events.ReleaseDetected{Email: "u@example.com", Repo: "o/r", ReleaseTag: "v1"})), nil)
	n.process(context.Background(), body, s)

	if !s.acked {
		t.Fatalf("expected ack for duplicate, got retry=%q dl=%q", s.retried, s.deadLettered)
	}
	if len(pub.cmds) != 0 {
		t.Errorf("expected no publish for duplicate, got %d", len(pub.cmds))
	}
	if len(dedup.unmarked) != 0 {
		t.Errorf("duplicate must not unmark, got %v", dedup.unmarked)
	}
}

func TestProcess_PublishFails_RollsBackDedupAndRetries(t *testing.T) {
	pub := &fakeCommandPublisher{err: errors.New("broker down")}
	dedup := &fakeDedup{fresh: true}
	n := New(pub, dedup)
	s := &fakeSettler{}

	env, body := mustEnvelope(t, must(events.NewReleaseDetected(events.ReleaseDetected{Email: "u@example.com", Repo: "o/r", ReleaseTag: "v1"})), nil)
	n.process(context.Background(), body, s)

	if s.retried == "" {
		t.Fatalf("expected retry, got ack=%v dl=%q", s.acked, s.deadLettered)
	}
	wantKey := dedupKeyPrefix + env.ID
	if len(dedup.unmarked) != 1 || dedup.unmarked[0] != wantKey {
		t.Errorf("expected dedup rollback of %s, got %v", wantKey, dedup.unmarked)
	}
}

func TestProcess_DedupError_Retries(t *testing.T) {
	pub := &fakeCommandPublisher{}
	dedup := &fakeDedup{markErr: errors.New("redis down")}
	n := New(pub, dedup)
	s := &fakeSettler{}

	_, body := mustEnvelope(t, must(events.NewReleaseDetected(events.ReleaseDetected{Email: "u@example.com", Repo: "o/r", ReleaseTag: "v1"})), nil)
	n.process(context.Background(), body, s)

	if s.retried == "" {
		t.Fatalf("expected retry on dedup error, got ack=%v dl=%q", s.acked, s.deadLettered)
	}
	if len(pub.cmds) != 0 {
		t.Errorf("expected no publish when dedup check fails, got %d", len(pub.cmds))
	}
}

func TestProcess_MalformedJSON_DeadLetters(t *testing.T) {
	pub := &fakeCommandPublisher{}
	dedup := &fakeDedup{fresh: true}
	n := New(pub, dedup)
	s := &fakeSettler{}

	n.process(context.Background(), []byte("{not json"), s)

	if s.deadLettered == "" {
		t.Fatalf("expected dead-letter, got ack=%v retry=%q", s.acked, s.retried)
	}
	if len(pub.cmds) != 0 || len(dedup.marked) != 0 {
		t.Errorf("malformed payload must not publish or mark dedup")
	}
}

func TestProcess_UnknownEventType_DeadLetters(t *testing.T) {
	pub := &fakeCommandPublisher{}
	dedup := &fakeDedup{fresh: true}
	n := New(pub, dedup)
	s := &fakeSettler{}

	body, err := json.Marshal(events.Envelope{ID: "abc", Type: "bogus.event", Version: 1, Payload: json.RawMessage("{}")})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	n.process(context.Background(), body, s)

	if s.deadLettered == "" {
		t.Fatalf("expected dead-letter, got ack=%v retry=%q", s.acked, s.retried)
	}
	if len(pub.cmds) != 0 || len(dedup.marked) != 0 {
		t.Errorf("unknown event must not publish or mark dedup")
	}
}

func must(env events.Envelope, err error) events.Envelope {
	if err != nil {
		panic(err)
	}
	return env
}