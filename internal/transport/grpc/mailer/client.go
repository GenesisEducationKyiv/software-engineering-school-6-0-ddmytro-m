package mailer

import (
	"context"
	"errors"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/mq"
	workermailer "github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/worker/mailer"
	mailerv1 "github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/proto/mailer/v1"
)

// Client publishes delivery commands to the mailer over gRPC. It satisfies the
// notifier's CommandPublisher interface, so the notifier core is transport-agnostic.
type Client struct {
	conn   *grpc.ClientConn
	client mailerv1.MailerServiceClient
}

// Dial connects to a mailer gRPC server at addr.
func Dial(addr string) (*Client, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn, client: mailerv1.NewMailerServiceClient(conn)}, nil
}

// Publish sends one command and returns an error if it was not delivered.
func (c *Client) Publish(ctx context.Context, cmd mq.DeliveryMessage) error {
	res, err := c.client.Send(ctx, toProto(cmd))
	if err != nil {
		return err
	}
	outcome := protoToOutcome[res.GetOutcome()]
	if outcome != workermailer.OutcomeDelivered {
		return errors.New("mailer did not deliver: " + outcome)
	}
	return nil
}

// Close releases the gRPC connection.
func (c *Client) Close() error {
	return c.conn.Close()
}
