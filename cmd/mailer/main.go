// Package main is the entry point for the mailer service.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/joho/godotenv"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/mq"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/redis"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/smtp"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/utils"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/worker/mailer"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	env := utils.GetEnv("APP_ENV", "development")

	err := godotenv.Load(fmt.Sprintf(".env.%s", env))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		log.Fatalf("error loading .env.%s: %v", env, err)
	}

	err = godotenv.Load()
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		log.Fatalf("error loading .env: %v", err)
	}

	redisClient := redis.GetClient(utils.GetEnv("REDIS_ADDR", "localhost:6379"))
	defer func() {
		if err := redisClient.Close(); err != nil {
			log.Printf("error closing Redis client: %v", err)
		}
	}()

	smtpUsername := utils.MustGetEnv("SMTP_USER")
	smtpClient := smtp.NewClient(
		utils.MustGetEnv("SMTP_HOST"),
		utils.GetEnvAs("SMTP_PORT", 587),
		smtpUsername,
		utils.MustGetEnv("SMTP_PASS"),
		utils.GetEnv("SMTP_FROM", smtpUsername),
		utils.GetEnv("SMTP_SENDER_EMAIL", smtpUsername),
	)

	stream := redis.NewStream(redisClient, mq.DeliveryStream)
	workerCount := utils.GetEnvAs("MAILER_WORKERS", 3)
	mlr := mailer.NewMailer(stream, "mailer_group", workerCount, smtpClient)

	log.Println("Mailer started")
	mlr.Start(ctx)
	log.Println("Mailer stopped")
}
