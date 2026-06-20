// Command intake: starts the gRPC PaymentIntake server.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/varadsat/distributed-payment-pipeline/internal/config"
	"github.com/varadsat/distributed-payment-pipeline/internal/kafka"
	"github.com/varadsat/distributed-payment-pipeline/internal/outbox"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg := config.Load()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, cfg.PostgresURL)
	if err != nil {
		logger.Error("failed to connect to postgres", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	producer, err := kafka.NewProducer(cfg.KafkaBroker) // your franz-go wrapper
	if err != nil {
		logger.Error("failed to create kafka producer", "error", err)
		os.Exit(1)
	}

	relay := outbox.NewPostgresRelay(pool, producer, logger)

	logger.Info("relay starting")
	if err := relay.Run(ctx); err != nil && ctx.Err() == nil {
		logger.Error("relay exited with error", "error", err)
		os.Exit(1)
	}
	logger.Info("relay shut down cleanly")
}
