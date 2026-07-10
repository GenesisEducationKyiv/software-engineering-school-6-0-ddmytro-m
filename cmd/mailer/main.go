// Package main is the entry point for the mailer service.
package main

import (
	"context"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/config"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/rabbitmq"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/smtp"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/logger"
	grpcmailer "github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/transport/grpc/mailer"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/worker/mailer"
	mailerv1 "github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/proto/mailer/v1"
)

func main() {
	logger.InitLogger()
	defer logger.Sync()

	if err := config.LoadEnv(); err != nil {
		logger.Log.Fatal("failed to load env", zap.Error(err))
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg := config.LoadMailerConfig()

	smtpClient := smtp.NewClient(
		cfg.SMTP.Host, cfg.SMTP.Port,
		cfg.SMTP.Username, cfg.SMTP.Password,
		cfg.SMTP.From, cfg.SMTP.SenderEmail,
	)

	retryPolicy := rabbitmq.NewRetryPolicy(
		time.Duration(cfg.RabbitMQ.RetryTTLSeconds)*time.Second,
		cfg.RabbitMQ.RetryBackoffFactor,
		cfg.RabbitMQ.MaxRetryAttempts,
	)

	conn := rabbitmq.Dial(cfg.RabbitMQ.URL, retryPolicy)
	defer func() {
		if closeErr := conn.Close(); closeErr != nil {
			logger.Log.Error("error closing RabbitMQ connection", zap.Error(closeErr))
		}
	}()

	results := mailer.NewResultPublisher(rabbitmq.NewPublisher(conn))
	mlr := mailer.New(smtpClient, results)
	consumer := rabbitmq.NewConsumer(
		conn,
		rabbitmq.CommandsEndpoint.Queues,
		cfg.PrefetchCount,
		retryPolicy,
		mlr.Handler(),
	)

	var wg sync.WaitGroup

	if cfg.GRPCEnabled {
		grpcServer, lis, listenErr := newGRPCServer(cfg.GRPCAddr, mlr)
		if listenErr != nil {
			logger.Log.Fatal("failed to listen for gRPC", zap.Error(listenErr))
		}
		wg.Go(func() {
			logger.Log.Info("Mailer gRPC server started", zap.String("addr", cfg.GRPCAddr))
			if serveErr := grpcServer.Serve(lis); serveErr != nil {
				logger.Log.Error("gRPC server stopped", zap.Error(serveErr))
			}
		})
		wg.Go(func() {
			<-ctx.Done()
			grpcServer.GracefulStop()
		})
	}

	for range cfg.Workers {
		wg.Go(func() {
			consumer.Start(ctx)
		})
	}

	logger.Log.Info("Mailer started", zap.Int("workers", cfg.Workers), zap.Bool("grpc", cfg.GRPCEnabled))
	<-ctx.Done()
	logger.Log.Info("Mailer shutting down, waiting for workers...")
	wg.Wait()
	logger.Log.Info("Mailer stopped")
}

// newGRPCServer builds a gRPC server exposing the mailer on the given address.
func newGRPCServer(addr string, d grpcmailer.Deliverer) (*grpc.Server, net.Listener, error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, nil, err
	}
	srv := grpc.NewServer()
	mailerv1.RegisterMailerServiceServer(srv, grpcmailer.NewServer(d))
	return srv, lis, nil
}
