package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ddmytro-m/github-scanner/internal/api/github"
	"github.com/ddmytro-m/github-scanner/internal/config"
	"github.com/ddmytro-m/github-scanner/internal/infra/db"
	"github.com/ddmytro-m/github-scanner/internal/infra/mq"
	redisDB "github.com/ddmytro-m/github-scanner/internal/infra/redis"
	"github.com/ddmytro-m/github-scanner/internal/infra/smtp"

	transportHttp "github.com/ddmytro-m/github-scanner/internal/transport/http"
	"github.com/ddmytro-m/github-scanner/internal/transport/http/handlers"
	"github.com/ddmytro-m/github-scanner/internal/worker/mailer"
	"github.com/ddmytro-m/github-scanner/internal/worker/scanner"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg := config.Get()

	orm := db.Get()
	defer db.Close()

	redisClient := redisDB.Get()
	defer redisClient.Close()

	ghClient := github.NewGitHubClient(
		cfg.GitHub.Token,
		&http.Client{Timeout: cfg.GitHub.Timeout},
		redisClient,
		cfg.GitHub.CacheTTL,
		cfg.GitHub.CacheErrorTTL,
	)

	emailMQ := mq.GetEmailMQ(redisClient)
	scn := scanner.NewScanner(orm, ghClient, emailMQ, &cfg.Scanner)

	smtpClient := smtp.NewClient(cfg.SMTP.Host, cfg.SMTP.Port, cfg.SMTP.Username, cfg.SMTP.Password, cfg.SMTP.From, cfg.SMTP.SenderEmail)

	stream := redisDB.NewRedisStream(redisClient, mq.DeliveryStream)
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
