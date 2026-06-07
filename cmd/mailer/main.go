// Package main is the entry point for the mailer service.
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/config"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/mq"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/redis"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/smtp"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/worker/mailer"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := config.LoadEnv(); err != nil {
		log.Fatal(err)
	}

	cfg := config.LoadMailerConfig()

	redisClient := redis.GetClient(cfg.Redis.Addr)
	defer func() {
		if err := redisClient.Close(); err != nil {
			log.Printf("error closing Redis client: %v", err)
		}
	}()

	smtpClient := smtp.NewClient(
		cfg.SMTP.Host, cfg.SMTP.Port,
		cfg.SMTP.Username, cfg.SMTP.Password,
		cfg.SMTP.From, cfg.SMTP.SenderEmail,
	)

	stream := redis.NewStream(redisClient, mq.DeliveryStream)
	mlr := mailer.NewMailer(stream, "mailer_group", cfg.Workers, smtpClient)

	log.Println("Mailer started")
	mlr.Start(ctx)
	log.Println("Mailer stopped")
}