// Package main is the entry point for the github-scanner server.
package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/api/github"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/config"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/db"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/eventpublisher"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/outbox"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/rabbitmq"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/redis"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/logger"

	transportHttp "github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/transport/http"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/transport/http/handlers"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/worker/scanner"
)

func main() {
	logger.InitLogger()
	defer logger.Sync()

	if err := config.LoadEnv(); err != nil {
		logger.Log.Fatal("failed to load env", zap.Error(err))
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg := config.LoadServerConfig()

	logger.Log.Info("configuration loaded", zap.String("environment", cfg.AppEnv))

	orm := db.Get()
	defer db.Close()

	redisClient := redis.GetClient(cfg.Redis.Addr)
	defer func() {
		err := redisClient.Close()
		if err != nil {
			logger.Log.Error("error closing Redis client", zap.Error(err))
		}
	}()

	cache := redis.NewCacheWithClient(redisClient)

	// GitHub API transport layers
	transport := http.DefaultTransport

	authTransport := github.NewAuthTransport(transport, cfg.GitHub.Token)
	rateLimitTransport := github.NewRateLimitTransport(authTransport, github.GetBaseRateLimits(cfg.GitHub.Token))
	cacheTransport := github.NewCacheTransport(rateLimitTransport, cache, cfg.GitHub.CacheTTL, cfg.GitHub.CacheErrorTTL)

	httpClient := &http.Client{
		Transport: cacheTransport,
		Timeout:   cfg.GitHub.Timeout,
	}

	ghClient := github.NewClient(
		github.WithHTTPClient(httpClient),
	)

	retryPolicy := rabbitmq.NewRetryPolicy(
		time.Duration(cfg.RabbitMQ.RetryTTLSeconds)*time.Second,
		cfg.RabbitMQ.RetryBackoffFactor,
		cfg.RabbitMQ.MaxRetryAttempts,
	)
	rmqConn, err := rabbitmq.Dial(cfg.RabbitMQ.URL, retryPolicy)
	if err != nil {
		logger.Log.Fatal("failed to connect to RabbitMQ", zap.Error(err))
	}
	defer func() {
		if closeErr := rmqConn.Close(); closeErr != nil {
			logger.Log.Error("error closing RabbitMQ connection", zap.Error(closeErr))
		}
	}()

	pub := rabbitmq.NewPublisher(rmqConn)
	eventPub := eventpublisher.New(pub)

	scn := scanner.NewScanner(orm, ghClient, eventPub, rateLimitTransport, &cfg.Scanner)

	subStore := handlers.NewSubscriptionStore(orm)
	subHandler := handlers.NewSubscriptionHandler(subStore, ghClient)
	srv := transportHttp.NewServer(":8080", subHandler)

	relay := outbox.NewRelay(orm, pub, cfg.Outbox.PollInterval, cfg.Outbox.BatchSize)

	go scn.Start(ctx)
	go relay.Run(ctx)
	go func() {
		if err := srv.Start(); err != nil {
			logger.Log.Error("HTTP server error", zap.Error(err))
		}
	}()

	logger.Log.Info("Scanner and HTTP Server are running...")
	<-ctx.Done()

	logger.Log.Info("shutting down...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Stop(shutdownCtx); err != nil {
		logger.Log.Error("HTTP server shutdown error", zap.Error(err))
	}

	time.Sleep(1 * time.Second)
}
