//go:build unit

package mailer

import (
	"context"
	"errors"
	"testing"

	"go.uber.org/zap"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/mq"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/logger"
	workermailer "github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/worker/mailer"
	mailerv1 "github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/proto/mailer/v1"
)

func init() { logger.Log = zap.NewNop() }

type fakeDeliverer struct {
	got []mq.DeliveryMessage
	err error
}

func (f *fakeDeliverer) Deliver(_ context.Context, msg mq.DeliveryMessage) error {
	f.got = append(f.got, msg)
	return f.err
}

func TestServerSend_Success(t *testing.T) {
	f := &fakeDeliverer{}
	res, err := NewServer(f).Send(context.Background(), &mailerv1.DeliveryCommand{
		Type:  mailerv1.EmailType_EMAIL_TYPE_NEW_RELEASE,
		Email: "u@example.com",
		Repo:  "owner/repo",
	})
	if err != nil {
		t.Fatalf("Send returned error: %v", err)
	}
	if !res.GetDelivered() {
		t.Error("Delivered = false, want true")
	}
	if len(f.got) != 1 || f.got[0].Event != mq.EventNewRelease {
		t.Errorf("deliverer received %+v, want one new_release", f.got)
	}
}

func TestServerSend_FailureOutcomes(t *testing.T) {
	cases := []struct {
		name       string
		err        error
		wantReason string
	}{
		{"poison", workermailer.ErrUnknownEmailType, workermailer.OutcomePoison},
		{"send failure", errors.New("smtp boom"), workermailer.OutcomeFailed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := NewServer(&fakeDeliverer{err: tc.err}).Send(
				context.Background(),
				&mailerv1.DeliveryCommand{Type: mailerv1.EmailType_EMAIL_TYPE_NEW_RELEASE, Email: "u@example.com"},
			)
			if err != nil {
				t.Fatalf("Send returned transport error: %v", err)
			}
			if res.GetDelivered() {
				t.Error("Delivered = true, want false")
			}
			if res.GetReason() != tc.wantReason {
				t.Errorf("Reason = %q, want %q", res.GetReason(), tc.wantReason)
			}
		})
	}
}
