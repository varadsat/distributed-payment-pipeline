// Command settlement: runs the settlement engine.
// Reads config from env, connects to Postgres, and calls engine.Run in a loop.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/varadsat/distributed-payment-pipeline/internal/settlement"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	// --- config (load from env in production via internal/config) ---
	dbURL := envOrDefault("DATABASE_URL", "postgres://payments:payments@localhost:5432/payments")
	cfg := settlement.Config{
		WindowSize: envDuration("SETTLEMENT_WINDOW_SIZE", 1*time.Hour),
		Buffer:     envDuration("SETTLEMENT_BUFFER", 5*time.Minute),
		Interval:   envDuration("SETTLEMENT_INTERVAL", 1*time.Minute),
	}

	// --- postgres pool ---
	pool, err := pgxpool.New(context.Background(), dbURL)
	if err != nil {
		logger.Error("failed to connect to postgres", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	// --- wire and run ---
	store := settlement.NewPgxStore(pool)
	engine := settlement.New(store, cfg, logger)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	logger.Info("starting settlement engine")
	if err := engine.Run(ctx); err != nil && err != context.Canceled {
		logger.Error("settlement engine exited with error", "err", err)
		os.Exit(1)
	}
	logger.Info("settlement engine stopped cleanly")
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
