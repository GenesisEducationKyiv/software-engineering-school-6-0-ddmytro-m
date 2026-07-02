//go:build unit

package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/events"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/db"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/logger"
)

func init() {
	logger.Log = zap.NewNop()
}

type fakeStore struct {
	states      map[string]db.SagaState
	completed   []string
	compensated []string
	canceled    []string

	lookupErr     error
	markErr       error
	compensateErr error
}

func newFakeStore() *fakeStore {
	return &fakeStore{states: map[string]db.SagaState{}}
}

func (f *fakeStore) SagaState(token string) (db.SagaState, error) {
	if f.lookupErr != nil {
		return "", f.lookupErr
	}
	st, ok := f.states[token]
	if !ok {
		return "", gorm.ErrRecordNotFound
	}
	return st, nil
}

func (f *fakeStore) MarkCompleted(token string) error {
	if f.markErr != nil {
		return f.markErr
	}
	f.completed = append(f.completed, token)
	f.states[token] = db.SagaCompleted
	return nil
}

// Compensate mimics the atomic cancel-subscription + mark-compensated store
// method: on failure, neither list is updated, matching the real
// implementation's all-or-nothing transaction.
func (f *fakeStore) Compensate(token string) error {
	if f.compensateErr != nil {
		return f.compensateErr
	}
	f.canceled = append(f.canceled, token)
	f.compensated = append(f.compensated, token)
	f.states[token] = db.SagaCompensated
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

func resultBody(env events.Envelope, err error) []byte {
	if err != nil {
		panic(err)
	}
	b, err := json.Marshal(env)
	if err != nil {
		panic(err)
	}
	return b
}

func TestProcess_Delivered_MarksCompleted(t *testing.T) {
	store := newFakeStore()
	store.states["tok"] = db.SagaAwaitingDelivery
	o := New(store)
	s := &fakeSettler{}

	o.process(context.Background(), resultBody(events.NewVerificationDelivered(events.VerificationDelivered{Token: "tok"})), s)

	if !s.acked {
		t.Fatalf("expected ack, retry=%q dl=%q", s.retried, s.deadLettered)
	}
	if len(store.completed) != 1 {
		t.Errorf("expected saga completed, got %v", store.completed)
	}
}

func TestProcess_Failed_Compensates(t *testing.T) {
	store := newFakeStore()
	store.states["tok"] = db.SagaAwaitingDelivery
	o := New(store)
	s := &fakeSettler{}

	o.process(context.Background(), resultBody(events.NewVerificationFailed(events.VerificationFailed{Token: "tok", Reason: "smtp"})), s)

	if !s.acked {
		t.Fatalf("expected ack, retry=%q dl=%q", s.retried, s.deadLettered)
	}
	if len(store.canceled) != 1 || store.canceled[0] != "tok" {
		t.Errorf("expected subscription cancel, got %v", store.canceled)
	}
	if len(store.compensated) != 1 {
		t.Errorf("expected saga compensated, got %v", store.compensated)
	}
}

func TestProcess_Delivered_UnknownSaga_Acks(t *testing.T) {
	store := newFakeStore() // no saga for tok
	o := New(store)
	s := &fakeSettler{}

	o.process(context.Background(), resultBody(events.NewVerificationDelivered(events.VerificationDelivered{Token: "tok"})), s)

	if !s.acked {
		t.Fatalf("expected ack for unknown saga, got retry=%q dl=%q", s.retried, s.deadLettered)
	}
	if len(store.completed) != 0 {
		t.Errorf("must not complete unknown saga, got %v", store.completed)
	}
}

func TestProcess_Failed_AlreadyTerminal_Acks(t *testing.T) {
	store := newFakeStore()
	store.states["tok"] = db.SagaCompleted // user already confirmed
	o := New(store)
	s := &fakeSettler{}

	o.process(context.Background(), resultBody(events.NewVerificationFailed(events.VerificationFailed{Token: "tok"})), s)

	if !s.acked {
		t.Fatalf("expected idempotent ack, got retry=%q dl=%q", s.retried, s.deadLettered)
	}
	if len(store.canceled) != 0 {
		t.Errorf("must not cancel an already-completed saga, got %v", store.canceled)
	}
}

func TestProcess_LookupError_Retries(t *testing.T) {
	store := newFakeStore()
	store.lookupErr = errors.New("db down")
	o := New(store)
	s := &fakeSettler{}

	o.process(context.Background(), resultBody(events.NewVerificationDelivered(events.VerificationDelivered{Token: "tok"})), s)

	if s.retried == "" {
		t.Fatalf("expected retry, got ack=%v dl=%q", s.acked, s.deadLettered)
	}
}

func TestProcess_CompensateFails_Retries(t *testing.T) {
	store := newFakeStore()
	store.states["tok"] = db.SagaAwaitingDelivery
	store.compensateErr = errors.New("db down")
	o := New(store)
	s := &fakeSettler{}

	o.process(context.Background(), resultBody(events.NewVerificationFailed(events.VerificationFailed{Token: "tok"})), s)

	if s.retried == "" {
		t.Fatalf("expected retry, got ack=%v dl=%q", s.acked, s.deadLettered)
	}
	if len(store.canceled) != 0 || len(store.compensated) != 0 {
		t.Errorf("must not partially compensate on failure, got canceled=%v compensated=%v", store.canceled, store.compensated)
	}
}

func TestProcess_MalformedJSON_DeadLetters(t *testing.T) {
	o := New(newFakeStore())
	s := &fakeSettler{}

	o.process(context.Background(), []byte("{not json"), s)

	if s.deadLettered == "" {
		t.Fatalf("expected dead-letter, got ack=%v retry=%q", s.acked, s.retried)
	}
}

func TestProcess_UnexpectedType_DeadLetters(t *testing.T) {
	o := New(newFakeStore())
	s := &fakeSettler{}

	body := resultBody(events.Envelope{Type: "release.detected", Version: 1, Payload: json.RawMessage("{}")}, nil)
	o.process(context.Background(), body, s)

	if s.deadLettered == "" {
		t.Fatalf("expected dead-letter, got ack=%v retry=%q", s.acked, s.retried)
	}
}
