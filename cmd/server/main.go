// Package main is the entry point for the github-scanner server.
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/api/github"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/config"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/db"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/mq"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/redis"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/smtp"

	transportHttp "github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/transport/http"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/transport/http/handlers"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/worker/mailer"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/worker/scanner"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg := config.Get()

	orm := db.Get()
	defer db.Close()

	redisClient := redis.Get()
	defer func() {
		err := redisClient.Close()
		if err != nil {
			log.Printf("error closing Redis client: %v", err)
		}
	}()

	cache := redis.NewCacheWithClient(redisClient)

	ghClient := github.NewClient(
		github.WithToken(cfg.GitHub.Token),
		github.WithHTTPClient(&http.Client{Timeout: cfg.GitHub.Timeout}),
		github.WithCache(
			cache,
			cfg.GitHub.CacheTTL,
			cfg.GitHub.CacheErrorTTL,
		),
	)

	emailMQ := mq.GetEmailMQ(redisClient)
	scn := scanner.NewScanner(orm, ghClient, emailMQ, &cfg.Scanner)

	smtpClient := smtp.NewClient(cfg.SMTP.Host, cfg.SMTP.Port, cfg.SMTP.Username, cfg.SMTP.Password, cfg.SMTP.From, cfg.SMTP.SenderEmail)

	stream := redis.NewStream(redisClient, mq.DeliveryStream)
	mlr := mailer.NewMailer(stream, "mailer_group", 3, smtpClient)

	subHandler := handlers.NewSubscriptionHandler(orm, ghClient, emailMQ)
	srv := transportHttp.NewServer(":8080", subHandler)

	go scn.Start(ctx)
	go mlr.Start(ctx)
	go func() {
		if err := srv.Start(); err != nil {
			log.Printf("HTTP server error: %v", err)
		}
	}()

	log.Println("Scanner, Mailer, and HTTP Server are running...")
	<-ctx.Done()

	log.Println("shutting down...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Stop(shutdownCtx); err != nil {
		log.Printf("HTTP server shutdown error: %v", err)
	}

	time.Sleep(1 * time.Second)
}
