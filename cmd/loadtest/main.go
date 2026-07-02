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
	"runtime"
	"sort"
	"strings"
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

// countingSender counts accepted messages so the harness can wait for a
// batch to drain without a fixed target fixed up front at construction time -
// letting the same sender/consumer pair serve both the warm-up phase and the
// timed phase back to back.
type countingSender struct {
	count atomic.Int64
}

func (s *countingSender) SendEmail(context.Context, string, string, string) error {
	s.count.Add(1)
	return nil
}

// waitForCount blocks until counted reaches at least want, or returns an
// error once timeout has elapsed.
func waitForCount(ctx context.Context, counted *atomic.Int64, want int64, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if counted.Load() >= want {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Millisecond):
		}
	}
	return fmt.Errorf("timed out: only %d/%d consumed", counted.Load(), want)
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
	warmup := flag.Int("warmup", 1000, "untimed messages sent before measuring, to prime connections/caches past cold-start; capped at n/10")
	stream := flag.Bool("stream", false, "grpc only: use the bidi SendStream RPC")
	rabbitURL := flag.String("rabbitmq-url", "amqp://guest:guest@localhost:5672/", "amqp only: broker URL")
	flag.Parse()

	logger.InitLogger()
	defer logger.Sync()

	ctx := context.Background()

	// Cap warm-up relative to n so a small -n run (e.g. a quick smoke test)
	// doesn't spend most of its time warming up.
	if maxWarmup := *n / 10; *warmup > maxWarmup {
		*warmup = maxWarmup
	}

	printHeader(*n, *warmup)

	switch *transport {
	case "grpc":
		return runGRPC(ctx, *n, *warmup, *stream)
	case "amqp":
		return runAMQP(ctx, *rabbitURL, *n, *warmup)
	default:
		return fmt.Errorf("unknown transport %q", *transport)
	}
}

// printHeader writes a comment header identifying the run: when, with what
// parameters, and on what hardware - so a saved log's numbers can be judged
// in context (e.g. a faster CPU or fewer cores would shift throughput) later.
func printHeader(n, warmup int) {
	fmt.Printf("# loadtest results  %s  n=%d warmup=%d\n", time.Now().UTC().Format(time.RFC3339), n, warmup)
	fmt.Printf("# go %s  host=%s  cpu=%q (%d cores)\n", runtime.Version(), runtime.GOARCH, cpuModel(), runtime.NumCPU())
}

// cpuModel returns a best-effort CPU model string. Linux exposes it via
// /proc/cpuinfo; other platforms have no portable stdlib equivalent, so they
// fall back to a generic label rather than shelling out to a platform tool.
func cpuModel() string {
	data, err := os.ReadFile("/proc/cpuinfo")
	if err != nil {
		return "unknown"
	}
	for line := range strings.Lines(string(data)) {
		rest, found := strings.CutPrefix(line, "model name")
		if !found {
			continue
		}
		if _, value, ok := strings.Cut(rest, ":"); ok {
			return strings.TrimSpace(value)
		}
	}
	return "unknown"
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
func runGRPC(ctx context.Context, n, warmup int, stream bool) error {
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
		return runGRPCStream(ctx, lis.Addr().String(), n, warmup)
	}

	client, err := grpcmailer.Dial(lis.Addr().String())
	if err != nil {
		return err
	}
	defer func() { _ = client.Close() }()

	for range warmup {
		if pubErr := client.Publish(ctx, sampleCommand); pubErr != nil {
			return fmt.Errorf("warm-up: %w", pubErr)
		}
	}

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

// runGRPCStream benchmarks the bidi SendStream RPC. Sending and receiving run
// on separate goroutines so the client pipelines requests instead of waiting
// for each response before sending the next - the actual advantage bidi
// streaming has over unary calls (a ping-pong send/recv loop on a stream
// gains nothing over per-message unary RPCs). grpc-go documents it as safe to
// call SendMsg and RecvMsg on the same stream from two different goroutines
// concurrently (just not SendMsg from two goroutines at once), so this split
// is safe as written.
//
// No per-message latency is reported here: with the sender racing ahead
// unthrottled, recv-time minus send-time mostly measures how deep the
// in-flight backlog got, not round-trip time (confirmed experimentally - it
// comes out ~1000x the unary p50 for identical work). Only aggregate
// throughput is a meaningful number for this shape of benchmark.
func runGRPCStream(ctx context.Context, addr string, n, warmup int) error {
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

	// Synchronous ping-pong is fine for warm-up: it's untimed, and simpler
	// than reasoning about a pipelined warm-up phase.
	for range warmup {
		if sendErr := stream.Send(cmd); sendErr != nil {
			return fmt.Errorf("warm-up: %w", sendErr)
		}
		if _, recvErr := stream.Recv(); recvErr != nil {
			return fmt.Errorf("warm-up: %w", recvErr)
		}
	}

	sendDone := make(chan error, 1)

	start := time.Now()
	go func() {
		for range n {
			if sendErr := stream.Send(cmd); sendErr != nil {
				sendDone <- sendErr
				return
			}
		}
		sendDone <- stream.CloseSend()
	}()

	for range n {
		if _, recvErr := stream.Recv(); recvErr != nil {
			return recvErr
		}
	}
	if sendErr := <-sendDone; sendErr != nil {
		return sendErr
	}
	report("grpc-stream", n, time.Since(start), nil)
	return nil
}

// runAMQP benchmarks the broker path by draining a published batch through a
// real mailer consumer with a counting sender.
func runAMQP(ctx context.Context, url string, n, warmup int) error {
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

	sender := &countingSender{}
	mlr := mailer.New(sender, nil)
	consumer := rabbitmq.NewConsumer(conn, rabbitmq.QueueSet{Main: queue}, 50, retry, mlr.Handler())

	consumeCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go consumer.Start(consumeCtx)

	publisher := rabbitmq.NewPublisher(conn)
	publishBatch := func(count int) error {
		for range count {
			if pubErr := publisher.Publish(ctx, rabbitmq.CommandsExchange, routingKey, sampleCommand); pubErr != nil {
				return pubErr
			}
		}
		return nil
	}

	if warmup > 0 {
		if err := publishBatch(warmup); err != nil {
			return fmt.Errorf("warm-up: %w", err)
		}
		if err := waitForCount(ctx, &sender.count, int64(warmup), 2*time.Minute); err != nil {
			return fmt.Errorf("warm-up: %w", err)
		}
	}

	start := time.Now()
	if err := publishBatch(n); err != nil {
		return err
	}
	if err := waitForCount(ctx, &sender.count, int64(warmup+n), 2*time.Minute); err != nil {
		return err
	}
	report("amqp", n, time.Since(start), nil)
	return nil
}
