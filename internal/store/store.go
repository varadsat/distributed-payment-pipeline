// Package store is the Postgres persistence layer.
package store

import (
	"context"

	"github.com/varadsat/distributed-payment-pipeline/internal/domain"
)

// Store persists transactions and writes outbox rows in the SAME transaction
// (the heart of the transactional-outbox pattern: no dual-write).
type Store interface {
	// SaveWithOutbox inserts the transaction row and an outbox row atomically.
	SaveWithOutbox(ctx context.Context, t domain.Transaction, outboxPayload []byte) error
	// UpdateState records a state transition and updates the row.
	UpdateState(ctx context.Context, paymentID string, from, to domain.State) error
	GetByPaymentID(ctx context.Context, paymentID string) (domain.Transaction, error)
	GetByIdempotencyKey(ctx context.Context, key string) (domain.Transaction, error)
}

// TODO: pgxStore implements Store using a pgxpool.Pool and a single tx in SaveWithOutbox.
