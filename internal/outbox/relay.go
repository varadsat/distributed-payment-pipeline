// Package outbox contains the relay that publishes unpublished outbox rows to
// Kafka and marks them published. Driven by Postgres LISTEN/NOTIFY plus a
// fallback poll so it reacts within milliseconds.
package outbox

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/varadsat/distributed-payment-pipeline/internal/kafka"
)

// Relay reads unpublished outbox rows, publishes them to Kafka, and marks them
// published on success. At-least-once delivery; consumers must be idempotent.
type Relay interface {
	Run(ctx context.Context) error
}

// TODO: implement poll-on-NOTIFY loop, batch publish, mark published in a tx.
type PostgresRelay struct {
	// DB connection, Kafka producer, etc.
	pool     *pgxpool.Pool
	producer kafka.Producer
	logger   *slog.Logger

	pollIntervalMs time.Duration
	batchSize      int
}

func NewPostgresRelay(pool *pgxpool.Pool, producer kafka.Producer, logger *slog.Logger) *PostgresRelay {
	return &PostgresRelay{
		pool:           pool,
		producer:       producer,
		logger:         logger,
		pollIntervalMs: 200 * time.Millisecond,
		batchSize:      100,
	}
}

func (r *PostgresRelay) Run(ctx context.Context) error {
	// TODO: implement the relay logic
	ticker := time.NewTicker(r.pollIntervalMs)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := r.relayBatch(ctx); err != nil {
				r.logger.Error("batch relay failed", "error", err)
			}
		case <-ctx.Done():
			r.logger.Info("relay shutting down")
			return nil
		}
	}
}

type outboxRow struct {
	ID           int64
	AggregateID  string
	Topic        string
	PartitionKey string
	Payload      []byte
}

func (r *PostgresRelay) relayBatch(ctx context.Context) error {
	// TODO: implement batch relay logic
	rows, err := r.pool.Query(ctx,
		`SELECT id, aggregate_id, topic, partition_key, payload
	 FROM outbox
	 WHERE published_at IS NULL
	 ORDER BY id
	 LIMIT $1`, r.batchSize)
	if err != nil {
		return err
	}
	defer rows.Close()

	var rowsList []outboxRow
	for rows.Next() {
		var row outboxRow
		if err := rows.Scan(&row.ID, &row.AggregateID, &row.Topic, &row.PartitionKey, &row.Payload); err != nil {
			return err
		}
		rowsList = append(rowsList, row)
	}

	if err := rows.Err(); err != nil {
		return err
	}

	for _, row := range rowsList {
		if err := r.producer.Publish(ctx, row.Topic, row.PartitionKey, row.Payload); err != nil {
			r.logger.Error("publish failed, stopping batch", "outbox_id", row.ID, "error", err)
			return err
		}
		_, err := r.pool.Exec(ctx, `UPDATE outbox SET published_at = NOW() WHERE id = $1`, row.ID)
		if err != nil {
			r.logger.Error("mark published failed", "outbox_id", row.ID, "error", err)
			return err
		}
		r.logger.Info("published and marked outbox row", "outbox_id", row.ID, "aggregate_id", row.AggregateID)

	}

	return nil
}
