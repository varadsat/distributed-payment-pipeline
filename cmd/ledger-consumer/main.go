// Command ledger-consumer: consumes payments.received and posts double-entry ledger records.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/varadsat/distributed-payment-pipeline/internal/config"
	"github.com/varadsat/distributed-payment-pipeline/internal/domain"
	"github.com/varadsat/distributed-payment-pipeline/internal/kafka"
	"github.com/varadsat/distributed-payment-pipeline/internal/ledger"
	"github.com/varadsat/distributed-payment-pipeline/internal/outbox"
	"github.com/varadsat/distributed-payment-pipeline/internal/store"
)

const consumerGroup = "ledger-consumer"

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

	consumer, err := kafka.NewConsumer(cfg.KafkaBroker)
	if err != nil {
		logger.Error("failed to create kafka consumer", "error", err)
		os.Exit(1)
	}

	dlqProducer, err := kafka.NewProducer(cfg.KafkaBroker)
	if err != nil {
		logger.Error("failed to create dlq producer", "error", err)
		os.Exit(1)
	}

	st, err := store.NewStore(ctx, cfg.PostgresURL)
	if err != nil {
		logger.Error("failed to create store", "error", err)
		os.Exit(1)
	}
	defer st.Close()

	poster := ledger.NewDoubleEntryPoster(pool, st, logger)

	handler := kafka.RetryHandler(
		handle(ctx, poster, logger),
		dlqProducer,
		kafka.TopicPaymentsReceived,
		consumerGroup,
		3,
	)

	logger.Info("ledger consumer starting")
	if err := consumer.Consume(ctx, kafka.TopicPaymentsReceived, consumerGroup, handler); err != nil && ctx.Err() == nil {
		logger.Error("consumer exited with error", "error", err)
		os.Exit(1)
	}
	logger.Info("ledger consumer shut down cleanly")
}

func handle(ctx context.Context, poster *ledger.DoubleEntryPoster, logger *slog.Logger) func(context.Context, string, []byte) error {
	return func(ctx context.Context, key string, payload []byte) error {
		var event outbox.PaymentReceivedEvent
		if err := json.Unmarshal(payload, &event); err != nil {
			logger.Warn("skipping unparseable event", "error", err, "payload", string(payload))
			return fmt.Errorf("%w: unmarshal: %w", kafka.ErrNonRetryable, err)
		}

		if event.PaymentID == "" || event.AccountID == "" || event.Currency == "" {
			logger.Warn("skipping malformed event: missing required fields",
				"payment_id", event.PaymentID,
				"account_id", event.AccountID,
				"currency", event.Currency,
			)
			return fmt.Errorf("%w: unmarshal: %w", kafka.ErrNonRetryable, errors.New("malformed event"))
		}

		trx := domain.Transaction{
			PaymentID: event.PaymentID,
			AccountID: event.AccountID,
			Amount: domain.Money{
				MinorUnits: event.AmountMinor,
				Currency:   event.Currency,
			},
			Source:    domain.Source(event.Source),
			State:     domain.State(event.State),
			CreatedAt: event.CreatedAt,
		}

		if err := poster.Post(ctx, trx); err != nil {
			return fmt.Errorf("post ledger entries payment_id=%s: %w", event.PaymentID, err)
		}

		logger.Info("ledger entries posted", "payment_id", event.PaymentID, "amount_minor", event.AmountMinor, "currency", event.Currency)
		return nil
	}
}
