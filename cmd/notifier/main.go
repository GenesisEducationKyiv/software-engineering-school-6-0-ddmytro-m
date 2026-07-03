// Package main is the entry point for the notifier service.
package main

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/config"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/rabbitmq"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/redis"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/logger"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/worker/notifier"
)

func main() {
	logger.InitLogger()
	defer logger.Sync()

	if err := config.LoadEnv(); err != nil {
		logger.Log.Fatal("failed to load env", zap.Error(err))
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg := config.LoadNotifierConfig()

	redisClient := redis.GetClient(cfg.Redis.Addr)
	defer func() {
		if err := redisClient.Close(); err != nil {
			logger.Log.Error("error closing Redis client", zap.Error(err))
		}
	}()

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

	publisher := notifier.NewCommandPublisher(rabbitmq.NewPublisher(conn))
	dedup := redis.NewDedup(redisClient, time.Duration(cfg.DedupTTLHours)*time.Hour)
	ntf := notifier.New(publisher, dedup)

	consumer := rabbitmq.NewConsumer(
		conn,
		rabbitmq.NotificationsEndpoint.Queues,
		cfg.PrefetchCount,
		retryPolicy,
		ntf.Handler(),
	)

	var wg sync.WaitGroup
	for range cfg.Workers {
		wg.Go(func() {
			consumer.Start(ctx)
		})
	}

	logger.Log.Info("Notifier started", zap.Int("workers", cfg.Workers))
	<-ctx.Done()
	logger.Log.Info("Notifier shutting down, waiting for workers...")
	wg.Wait()
	logger.Log.Info("Notifier stopped")
}
