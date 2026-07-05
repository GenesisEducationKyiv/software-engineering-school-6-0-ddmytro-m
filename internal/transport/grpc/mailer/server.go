package mailer

import (
	"context"
	"errors"
	"io"
	"time"

	"go.uber.org/zap"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/mq"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/logger"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/metrics"
	workermailer "github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/worker/mailer"
	mailerv1 "github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/proto/mailer/v1"
)

// Deliverer sends one delivery message. Satisfied by *workermailer.Mailer; the
// server depends on this narrow abstraction, not the concrete mailer.
type Deliverer interface {
	Deliver(ctx context.Context, msg mq.DeliveryMessage) error
}

// Server adapts a Deliverer onto the MailerService gRPC API.
type Server struct {
	mailerv1.UnimplementedMailerServiceServer
	deliverer Deliverer
}

// NewServer creates a gRPC mailer server backed by the given Deliverer.
func NewServer(d Deliverer) *Server {
	return &Server{deliverer: d}
}

// Send delivers one command and returns its result.
func (s *Server) Send(ctx context.Context, cmd *mailerv1.DeliveryCommand) (*mailerv1.SendResult, error) {
	return s.deliver(ctx, cmd), nil
}

// SendStream delivers a stream of commands, returning one result per command.
// A single delivery failure does not abort the stream.
func (s *Server) SendStream(stream mailerv1.MailerService_SendStreamServer) error {
	ctx := stream.Context()
	for {
		cmd, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if sendErr := stream.Send(s.deliver(ctx, cmd)); sendErr != nil {
			return sendErr
		}
	}
}

// deliver runs one command through the Deliverer, records the transport metric,
// and maps the outcome to a SendResult response code.
func (s *Server) deliver(ctx context.Context, cmd *mailerv1.DeliveryCommand) *mailerv1.SendResult {
	msg := fromProto(cmd)
	start := time.Now()
	err := s.deliverer.Deliver(ctx, msg)
	outcome := workermailer.DeliveryOutcome(err)
	metrics.ObserveMailerDelivery(metrics.TransportGRPC, outcome, time.Since(start).Seconds())

	if err != nil {
		logger.Log.Debug("grpc delivery not completed", zap.String("outcome", outcome))
	}
	return &mailerv1.SendResult{Outcome: outcomeToProto[outcome]}
}
