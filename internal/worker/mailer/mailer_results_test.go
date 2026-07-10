//go:build unit

package mailer

import (
	"context"
	"errors"
	"testing"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/mq"
)

type failedResult struct {
	token, reason string
}

type fakeResults struct {
	delivered []string
	failed    []failedResult

	failCalls int // fail this many calls total (across both methods), then succeed
	calls     int
}

func (f *fakeResults) VerificationDelivered(_ context.Context, token string) error {
	f.calls++
	if f.calls <= f.failCalls {
		return errors.New("publish boom")
	}
	f.delivered = append(f.delivered, token)
	return nil
}

func (f *fakeResults) VerificationFailed(_ context.Context, token, reason string) error {
	f.calls++
	if f.calls <= f.failCalls {
		return errors.New("publish boom")
	}
	f.failed = append(f.failed, failedResult{token, reason})
	return nil
}

func newMailerWithResults(sender EmailSender, results ResultPublisher) *Mailer {
	m := New(sender, results)
	m.baseBackoff = 0
	return m
}

func verificationCmd() mq.DeliveryMessage {
	return mq.DeliveryMessage{
		Event:   mq.EventEmailVerification,
		Email:   "u@example.com",
		Payload: map[string]any{"token": "tok123"},
	}
}

func TestProcess_VerificationSuccess_PublishesDelivered(t *testing.T) {
	results := &fakeResults{}
	m := newMailerWithResults(&fakeSender{}, results)
	s := &fakeSettler{}

	m.process(context.Background(), mustJSON(t, verificationCmd()), s)

	if !s.acked {
		t.Fatalf("expected ack, got retry=%q dl=%q", s.retried, s.deadLettered)
	}
	if len(results.delivered) != 1 || results.delivered[0] != "tok123" {
		t.Errorf("expected delivered result for tok123, got %v", results.delivered)
	}
	if len(results.failed) != 0 {
		t.Errorf("expected no failed result, got %v", results.failed)
	}
}

func TestProcess_VerificationFailure_PublishesFailedAndAcks(t *testing.T) {
	results := &fakeResults{}
	m := newMailerWithResults(&fakeSender{failures: 1000}, results) // always fails
	s := &fakeSettler{}

	m.process(context.Background(), mustJSON(t, verificationCmd()), s)

	// Terminal for the saga: ack, not retry.
	if !s.acked || s.retried != "" {
		t.Fatalf("expected terminal ack, got ack=%v retry=%q", s.acked, s.retried)
	}
	if len(results.failed) != 1 || results.failed[0].token != "tok123" {
		t.Errorf("expected failed result for tok123, got %v", results.failed)
	}
}

func TestProcess_VerificationSuccess_ReportRetriesThenSucceeds(t *testing.T) {
	results := &fakeResults{failCalls: 2} // fail twice, succeed on third
	m := newMailerWithResults(&fakeSender{}, results)
	s := &fakeSettler{}

	m.process(context.Background(), mustJSON(t, verificationCmd()), s)

	if !s.acked {
		t.Fatalf("expected ack, got retry=%q dl=%q", s.retried, s.deadLettered)
	}
	if len(results.delivered) != 1 || results.delivered[0] != "tok123" {
		t.Errorf("expected delivered result after retries, got %v", results.delivered)
	}
	if results.calls != 3 {
		t.Errorf("expected 3 report attempts, got %d", results.calls)
	}
}

func TestProcess_VerificationSuccess_ReportFailsAfterRetries_StillAcks(t *testing.T) {
	results := &fakeResults{failCalls: 1000} // always fails
	m := newMailerWithResults(&fakeSender{}, results)
	s := &fakeSettler{}

	m.process(context.Background(), mustJSON(t, verificationCmd()), s)

	// The email was already sent; acking (not retrying the whole command) avoids
	// a duplicate send even though the outcome couldn't be reported.
	if !s.acked || s.retried != "" {
		t.Fatalf("expected terminal ack despite report failure, got ack=%v retry=%q", s.acked, s.retried)
	}
	if len(results.delivered) != 0 {
		t.Errorf("expected no delivered result recorded, got %v", results.delivered)
	}
	if results.calls != maxResultReportRetries {
		t.Errorf("expected %d report attempts, got %d", maxResultReportRetries, results.calls)
	}
}

func TestProcess_VerificationFailure_ReportRetriesThenSucceeds(t *testing.T) {
	results := &fakeResults{failCalls: 2}
	m := newMailerWithResults(&fakeSender{failures: 1000}, results) // SMTP always fails
	s := &fakeSettler{}

	m.process(context.Background(), mustJSON(t, verificationCmd()), s)

	if !s.acked || s.retried != "" {
		t.Fatalf("expected terminal ack, got ack=%v retry=%q", s.acked, s.retried)
	}
	if len(results.failed) != 1 || results.failed[0].token != "tok123" {
		t.Errorf("expected failed result after retries, got %v", results.failed)
	}
	if results.calls != 3 {
		t.Errorf("expected 3 report attempts, got %d", results.calls)
	}
}

func TestProcess_NonVerificationFailure_RetriesWithoutResult(t *testing.T) {
	results := &fakeResults{}
	m := newMailerWithResults(&fakeSender{failures: 1000}, results)
	s := &fakeSettler{}

	cmd := mq.DeliveryMessage{Event: mq.EventNewRelease, Email: "u@example.com", Repo: "o/r", Release: "v1"}
	m.process(context.Background(), mustJSON(t, cmd), s)

	if s.retried == "" {
		t.Fatalf("expected broker retry for non-verification, got ack=%v", s.acked)
	}
	if len(results.delivered) != 0 || len(results.failed) != 0 {
		t.Errorf("non-verification must not emit saga results, got delivered=%v failed=%v", results.delivered, results.failed)
	}
}
