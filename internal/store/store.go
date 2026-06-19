// Package store is the Postgres persistence layer.
package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
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
	Save(ctx context.Context, t domain.Transaction) error
	Close()
}

// TODO: pgxStore implements Store using a pgxpool.Pool and a single tx in SaveWithOutbox.
type pgxStore struct {
	pool *pgxpool.Pool
}

func NewStore(ctx context.Context, databaseURL string) (*pgxStore, error) {
	if databaseURL == "" {
		return nil, errors.New("database URL is required")
	}

	dbpool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("create connection pool: %w", err)
	}

	return &pgxStore{pool: dbpool}, nil
}

func (s *pgxStore) GetByPaymentID(ctx context.Context, paymentID string) (domain.Transaction, error) {
	var tx domain.Transaction

	err := s.pool.QueryRow(ctx, `
        SELECT
            payment_id,
            idempotency_key,
            source,
            external_txn_id,
            account_id,
            amount_minor,
            currency,
            state,
            schema_version,
            metadata,
            created_at,
            updated_at
        FROM transactions
        WHERE payment_id = $1
    `, paymentID).Scan(
		&tx.PaymentID,
		&tx.IdempotencyKey,
		&tx.Source,
		&tx.ExternalTxnID,
		&tx.AccountID,
		&tx.Amount.MinorUnits,
		&tx.Amount.Currency,
		&tx.State,
		&tx.SchemaVersion,
		&tx.Metadata,
		&tx.CreatedAt,
		&tx.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.Transaction{}, fmt.Errorf("transaction not found for payment_id=%s", paymentID)
		}
		return domain.Transaction{}, fmt.Errorf("query transaction by payment_id: %w", err)
	}

	return tx, nil
}

func (s *pgxStore) GetByIdempotencyKey(ctx context.Context, key string) (domain.Transaction, error) {
	var tx domain.Transaction

	err := s.pool.QueryRow(ctx, `
        SELECT
            payment_id,
            idempotency_key,
            source,
            external_txn_id,
            account_id,
            amount_minor,
            currency,
            state,
            schema_version,
            metadata,
            created_at,
            updated_at
        FROM transactions
        WHERE idempotency_key = $1
    `, key).Scan(
		&tx.PaymentID,
		&tx.IdempotencyKey,
		&tx.Source,
		&tx.ExternalTxnID,
		&tx.AccountID,
		&tx.Amount.MinorUnits,
		&tx.Amount.Currency,
		&tx.State,
		&tx.SchemaVersion,
		&tx.Metadata,
		&tx.CreatedAt,
		&tx.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.Transaction{}, fmt.Errorf("transaction not found for idempotency_key=%s", key)
		}
		return domain.Transaction{}, fmt.Errorf("query transaction by idempotency_key: %w", err)
	}

	return tx, nil
}

func (s *pgxStore) Save(ctx context.Context, t domain.Transaction) error {
	_, err := s.pool.Exec(ctx, `
        INSERT INTO transactions (
            payment_id,
            idempotency_key,
            source,
            external_txn_id,
            account_id,
            amount_minor,
            currency,
            state,
            schema_version,
            metadata,
            created_at,
            updated_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
        )
    `,
		t.PaymentID,
		t.IdempotencyKey,
		t.Source,
		t.ExternalTxnID,
		t.AccountID,
		t.Amount.MinorUnits,
		t.Amount.Currency,
		t.State,
		t.SchemaVersion,
		t.Metadata,
		t.CreatedAt,
		t.UpdatedAt, 
	)
	if err != nil {
		return fmt.Errorf("insert transaction: %w", err)
	}

	return nil
}

// implement a Close function to close the connection pool when the application shuts down.
func (s *pgxStore) Close() {
	s.pool.Close()
}

func (s *pgxStore) SaveWithOutbox(ctx context.Context, t domain.Transaction, outboxPayload []byte) error {
	return nil
}

func (s *pgxStore) UpdateState(ctx context.Context, paymentID string, from, to domain.State) error {
	return nil
}
