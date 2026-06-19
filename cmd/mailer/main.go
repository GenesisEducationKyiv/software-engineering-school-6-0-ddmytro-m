// Package main is the entry point for the mailer service.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/config"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/contract"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/redis"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/smtp"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/logger"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/worker/mailer"
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

	redisClient := redis.GetClient(cfg.Redis.Addr)
	defer func() {
		if err := redisClient.Close(); err != nil {
			logger.Log.Error("error closing Redis client", zap.Error(err))
		}
	}()

	smtpClient := smtp.NewClient(
		cfg.SMTP.Host, cfg.SMTP.Port,
		cfg.SMTP.Username, cfg.SMTP.Password,
		cfg.SMTP.From, cfg.SMTP.SenderEmail,
	)

	stream := redis.NewStream(redisClient, contract.DeliveryStream)
	mlr := mailer.NewMailer(stream, "mailer_group", cfg.Workers, smtpClient)

	logger.Log.Info("Mailer started")
	mlr.Start(ctx)
	logger.Log.Info("Mailer stopped")
}
