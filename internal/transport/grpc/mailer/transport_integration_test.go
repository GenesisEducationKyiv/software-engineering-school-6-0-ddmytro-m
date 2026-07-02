//go:build integration

package mailer

import (
	"context"
	"net"
	"sync"
	"testing"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/mq"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/logger"
	workermailer "github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/worker/mailer"
	mailerv1 "github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/proto/mailer/v1"
)

func init() { logger.Log = zap.NewNop() }

type recordingSender struct {
	mu   sync.Mutex
	sent []string
}

func (r *recordingSender) SendEmail(_ context.Context, to, _, _ string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sent = append(r.sent, to)
	return nil
}

func (r *recordingSender) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.sent)
}

// startServer spins up a real mailer behind a gRPC server on a loopback port.
func startServer(t *testing.T) (addr string, sender *recordingSender) {
	t.Helper()
	sender = &recordingSender{}
	mlr := workermailer.New(sender, nil)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	mailerv1.RegisterMailerServiceServer(srv, NewServer(mlr))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String(), sender
}

func TestGRPC_Publish_DeliversEmail(t *testing.T) {
	addr, sender := startServer(t)
	client, err := Dial(addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	cmd := mq.DeliveryMessage{Event: mq.EventNewRelease, Email: "u@example.com", Repo: "o/r", Release: "v1"}
	if pubErr := client.Publish(context.Background(), cmd); pubErr != nil {
		t.Fatalf("Publish: %v", pubErr)
	}
	if sender.count() != 1 {
		t.Errorf("sender called %d times, want 1", sender.count())
	}
}

func TestGRPC_Publish_UnknownEvent_ReturnsError(t *testing.T) {
	addr, sender := startServer(t)
	client, err := Dial(addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	cmd := mq.DeliveryMessage{Event: "bogus", Email: "u@example.com"}
	if pubErr := client.Publish(context.Background(), cmd); pubErr == nil {
		t.Fatal("Publish of unknown event should return an error")
	}
	if sender.count() != 0 {
		t.Errorf("sender called %d times for poison command, want 0", sender.count())
	}
}

func TestGRPC_SendStream_DeliversBatch(t *testing.T) {
	addr, sender := startServer(t)
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	stream, err := mailerv1.NewMailerServiceClient(conn).SendStream(context.Background())
	if err != nil {
		t.Fatalf("SendStream: %v", err)
	}
	const batch = 3
	for range batch {
		cmd := &mailerv1.DeliveryCommand{Type: mailerv1.EmailType_EMAIL_TYPE_NEW_RELEASE, Email: "u@example.com", Repo: "o/r", Release: "v1"}
		if sendErr := stream.Send(cmd); sendErr != nil {
			t.Fatalf("stream send: %v", sendErr)
		}
		res, recvErr := stream.Recv()
		if recvErr != nil {
			t.Fatalf("stream recv: %v", recvErr)
		}
		if !res.GetDelivered() {
			t.Errorf("result Delivered = false, reason %q", res.GetReason())
		}
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("close send: %v", err)
	}
	if sender.count() != batch {
		t.Errorf("sender called %d times, want %d", sender.count(), batch)
	}
}
