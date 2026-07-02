// Package main is a load harness comparing the notifier -> mailer transports.
// It drives a fixed number of delivery commands through either the gRPC or the
// AMQP path against a no-op email sender, so the numbers reflect transport
// overhead rather than SMTP latency.
package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"sort"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/mq"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/rabbitmq"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/logger"
	grpcmailer "github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/transport/grpc/mailer"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/worker/mailer"
	mailerv1 "github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/proto/mailer/v1"
)

// sampleCommand is the workload unit sent through each transport.
var sampleCommand = mq.DeliveryMessage{
	Event:   mq.EventNewRelease,
	Email:   "load@example.com",
	Repo:    "owner/repo",
	Release: "v1.0.0",
}

// protoSample is the gRPC encoding of sampleCommand for the streaming path.
func protoSample() *mailerv1.DeliveryCommand {
	return &mailerv1.DeliveryCommand{
		Type:    mailerv1.EmailType_EMAIL_TYPE_NEW_RELEASE,
		Email:   sampleCommand.Email,
		Repo:    sampleCommand.Repo,
		Release: sampleCommand.Release,
	}
}

// noopSender satisfies mailer.EmailSender without doing any I/O.
type noopSender struct{}

func (noopSender) SendEmail(context.Context, string, string, string) error { return nil }

// countingSender signals once it has accepted target messages; used to know
// when the AMQP consumer has drained the published batch.
type countingSender struct {
	count  atomic.Int64
	target int64
	done   chan struct{}
}

func (s *countingSender) SendEmail(context.Context, string, string, string) error {
	if s.count.Add(1) == s.target {
		close(s.done)
	}
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "loadtest failed:", err)
		os.Exit(1)
	}
}

func run() error {
	transport := flag.String("transport", "grpc", "transport to benchmark: grpc | amqp")
	n := flag.Int("n", 10000, "number of delivery commands to send")
	stream := flag.Bool("stream", false, "grpc only: use the bidi SendStream RPC")
	rabbitURL := flag.String("rabbitmq-url", "amqp://guest:guest@localhost:5672/", "amqp only: broker URL")
	flag.Parse()

	logger.InitLogger()
	defer logger.Sync()

	ctx := context.Background()

	switch *transport {
	case "grpc":
		return runGRPC(ctx, *n, *stream)
	case "amqp":
		return runAMQP(ctx, *rabbitURL, *n)
	default:
		return fmt.Errorf("unknown transport %q", *transport)
	}
}

// report prints a throughput summary.
func report(transport string, n int, elapsed time.Duration, latencies []time.Duration) {
	rate := float64(n) / elapsed.Seconds()
	fmt.Printf("transport=%s n=%d elapsed=%s throughput=%.0f msg/s\n", transport, n, elapsed.Round(time.Millisecond), rate)
	if len(latencies) > 0 {
		sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
		fmt.Printf("  latency p50=%s p99=%s\n", percentile(latencies, 0.50), percentile(latencies, 0.99))
	}
}

// percentile returns the q-quantile of a sorted slice.
func percentile(sorted []time.Duration, q float64) time.Duration {
	idx := int(q * float64(len(sorted)-1))
	return sorted[idx].Round(time.Microsecond)
}

// runGRPC benchmarks the gRPC transport with an in-process server.
func runGRPC(ctx context.Context, n int, stream bool) error {
	mlr := mailer.New(noopSender{}, nil)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	srv := grpc.NewServer()
	mailerv1.RegisterMailerServiceServer(srv, grpcmailer.NewServer(mlr))
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()

	if stream {
		return runGRPCStream(ctx, lis.Addr().String(), n)
	}

	client, err := grpcmailer.Dial(lis.Addr().String())
	if err != nil {
		return err
	}
	defer func() { _ = client.Close() }()

	latencies := make([]time.Duration, 0, n)
	start := time.Now()
	for range n {
		callStart := time.Now()
		if pubErr := client.Publish(ctx, sampleCommand); pubErr != nil {
			return pubErr
		}
		latencies = append(latencies, time.Since(callStart))
	}
	report("grpc-unary", n, time.Since(start), latencies)
	return nil
}

// runGRPCStream benchmarks the bidi SendStream RPC.
func runGRPCStream(ctx context.Context, addr string, n int) error {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	stream, err := mailerv1.NewMailerServiceClient(conn).SendStream(ctx)
	if err != nil {
		return err
	}
	cmd := protoSample()

	start := time.Now()
	for range n {
		if sendErr := stream.Send(cmd); sendErr != nil {
			return sendErr
		}
		if _, recvErr := stream.Recv(); recvErr != nil {
			return recvErr
		}
	}
	if closeErr := stream.CloseSend(); closeErr != nil {
		return closeErr
	}
	report("grpc-stream", n, time.Since(start), nil)
	return nil
}

// runAMQP benchmarks the broker path by draining a published batch through a
// real mailer consumer with a counting sender.
func runAMQP(ctx context.Context, url string, n int) error {
	retry := rabbitmq.NewRetryPolicy(30*time.Second, 2, 5)
	conn := rabbitmq.Dial(url, retry)
	defer func() { _ = conn.Close() }()

	// Use a private queue and routing key so the harness drains its own batch
	// instead of competing with a running mailer service on email.delivery.
	routingKey := fmt.Sprintf("loadtest.%d", os.Getpid())
	queue := fmt.Sprintf("loadtest.%d", os.Getpid())
	if declErr := conn.DeclareEphemeralQueue(rabbitmq.CommandsExchange, queue, routingKey); declErr != nil {
		return declErr
	}

	sender := &countingSender{target: int64(n), done: make(chan struct{})}
	mlr := mailer.New(sender, nil)
	consumer := rabbitmq.NewConsumer(conn, rabbitmq.QueueSet{Main: queue}, 50, retry, mlr.Handler())

	consumeCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go consumer.Start(consumeCtx)

	publisher := rabbitmq.NewPublisher(conn)
	start := time.Now()
	for range n {
		if pubErr := publisher.Publish(ctx, rabbitmq.CommandsExchange, routingKey, sampleCommand); pubErr != nil {
			return pubErr
		}
	}

	select {
	case <-sender.done:
	case <-time.After(2 * time.Minute):
		return fmt.Errorf("timed out: only %d/%d consumed", sender.count.Load(), n)
	}
	report("amqp", n, time.Since(start), nil)
	return nil
}
