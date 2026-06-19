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
	created     []string
	deleted     []string
	completed   []string
	compensated []string
	canceled    []string

	createErr error
	lookupErr error
	markErr   error
	cancelErr error
}

func newFakeStore() *fakeStore {
	return &fakeStore{states: map[string]db.SagaState{}}
}

func (f *fakeStore) CreateSaga(token string) error {
	if f.createErr != nil {
		return f.createErr
	}
	f.created = append(f.created, token)
	f.states[token] = db.SagaAwaitingDelivery
	return nil
}

func (f *fakeStore) DeleteSaga(token string) error {
	f.deleted = append(f.deleted, token)
	delete(f.states, token)
	return nil
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

func (f *fakeStore) MarkCompensated(token string) error {
	if f.markErr != nil {
		return f.markErr
	}
	f.compensated = append(f.compensated, token)
	f.states[token] = db.SagaCompensated
	return nil
}

func (f *fakeStore) CancelPendingSubscription(token string) error {
	if f.cancelErr != nil {
		return f.cancelErr
	}
	f.canceled = append(f.canceled, token)
	return nil
}

type fakePublisher struct {
	tokens []string
	err    error
}

func (p *fakePublisher) SendEmailVerification(_ /*email*/, token string) error {
	p.tokens = append(p.tokens, token)
	return p.err
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

func TestSendEmailVerification_StartsSaga(t *testing.T) {
	store := newFakeStore()
	pub := &fakePublisher{}
	o := New(store, pub)

	if err := o.SendEmailVerification("u@example.com", "tok"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(store.created) != 1 || store.created[0] != "tok" {
		t.Errorf("saga not created: %v", store.created)
	}
	if len(pub.tokens) != 1 || pub.tokens[0] != "tok" {
		t.Errorf("event not published: %v", pub.tokens)
	}
	if len(store.deleted) != 0 {
		t.Errorf("saga should not be rolled back, got %v", store.deleted)
	}
}

func TestSendEmailVerification_PublishFails_RollsBackSaga(t *testing.T) {
	store := newFakeStore()
	pub := &fakePublisher{err: errors.New("broker down")}
	o := New(store, pub)

	if err := o.SendEmailVerification("u@example.com", "tok"); err == nil {
		t.Fatal("expected error")
	}
	if len(store.deleted) != 1 || store.deleted[0] != "tok" {
		t.Errorf("expected saga rollback, got %v", store.deleted)
	}
}

func TestSendEmailVerification_CreateFails_NoPublish(t *testing.T) {
	store := newFakeStore()
	store.createErr = errors.New("db down")
	pub := &fakePublisher{}
	o := New(store, pub)

	if err := o.SendEmailVerification("u@example.com", "tok"); err == nil {
		t.Fatal("expected error")
	}
	if len(pub.tokens) != 0 {
		t.Errorf("publish must not run when saga create fails, got %v", pub.tokens)
	}
}

func TestProcess_Delivered_MarksCompleted(t *testing.T) {
	store := newFakeStore()
	store.states["tok"] = db.SagaAwaitingDelivery
	o := New(store, &fakePublisher{})
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
	o := New(store, &fakePublisher{})
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
	o := New(store, &fakePublisher{})
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
	o := New(store, &fakePublisher{})
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
	o := New(store, &fakePublisher{})
	s := &fakeSettler{}

	o.process(context.Background(), resultBody(events.NewVerificationDelivered(events.VerificationDelivered{Token: "tok"})), s)

	if s.retried == "" {
		t.Fatalf("expected retry, got ack=%v dl=%q", s.acked, s.deadLettered)
	}
}

func TestProcess_CancelFails_Retries(t *testing.T) {
	store := newFakeStore()
	store.states["tok"] = db.SagaAwaitingDelivery
	store.cancelErr = errors.New("db down")
	o := New(store, &fakePublisher{})
	s := &fakeSettler{}

	o.process(context.Background(), resultBody(events.NewVerificationFailed(events.VerificationFailed{Token: "tok"})), s)

	if s.retried == "" {
		t.Fatalf("expected retry, got ack=%v dl=%q", s.acked, s.deadLettered)
	}
	if len(store.compensated) != 0 {
		t.Errorf("must not mark compensated when cancel failed, got %v", store.compensated)
	}
}

func TestProcess_MalformedJSON_DeadLetters(t *testing.T) {
	o := New(newFakeStore(), &fakePublisher{})
	s := &fakeSettler{}

	o.process(context.Background(), []byte("{not json"), s)

	if s.deadLettered == "" {
		t.Fatalf("expected dead-letter, got ack=%v retry=%q", s.acked, s.retried)
	}
}

func TestProcess_UnexpectedType_DeadLetters(t *testing.T) {
	o := New(newFakeStore(), &fakePublisher{})
	s := &fakeSettler{}

	body := resultBody(events.Envelope{Type: "release.detected", Version: 1, Payload: json.RawMessage("{}")}, nil)
	o.process(context.Background(), body, s)

	if s.deadLettered == "" {
		t.Fatalf("expected dead-letter, got ack=%v retry=%q", s.acked, s.retried)
	}
}
