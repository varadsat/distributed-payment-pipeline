// Command notification-consumer: consumes payments.received and stubs a notification systen.
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

	"github.com/varadsat/distributed-payment-pipeline/internal/config"
	"github.com/varadsat/distributed-payment-pipeline/internal/domain"
	"github.com/varadsat/distributed-payment-pipeline/internal/kafka"
	"github.com/varadsat/distributed-payment-pipeline/internal/outbox"
)

const consumerGroup = "notification-consumer"

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg := config.Load()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

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

	handler := kafka.RetryHandler(
		handle(ctx, logger),
		dlqProducer,
		kafka.TopicPaymentsReceived,
		consumerGroup,
		3,
	)

	logger.Info("notification consumer starting")
	if err := consumer.Consume(ctx, kafka.TopicPaymentsReceived, consumerGroup, handler); err != nil && ctx.Err() == nil {
		logger.Error("consumer exited with error", "error", err)
		os.Exit(1)
	}
	logger.Info("notification consumer shut down cleanly")
}

func handle(ctx context.Context, logger *slog.Logger) func(context.Context, string, []byte) error {
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

		logger.Info("notification entries sent (STUB)", "payment_id", trx.PaymentID, "account_id", trx.AccountID, "amount", trx.Amount)
		return nil
	}
}
