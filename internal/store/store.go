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
	// Start a transaction
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			tx.Rollback(ctx)
		} else {
			tx.Commit(ctx)
		}
	}()

	// Insert the transaction row
	_, err = tx.Exec(ctx, `
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

	// Insert the outbox row
	_, err = tx.Exec(ctx, `
		INSERT INTO outbox (
			aggregate_id,
			topic,
			partition_key,
			payload,
			created_at
		) VALUES (

			$1, $2, $3, $4, $5
		)
	`, t.PaymentID, "payments.received", t.AccountID, outboxPayload, t.CreatedAt)
	if err != nil {
		return fmt.Errorf("insert outbox: %w", err)
	}

	return nil
}

func (s *pgxStore) UpdateState(ctx context.Context, paymentID string, from, to domain.State) error {
	// 1. Validate the transition against the state machine before touching the DB.
	if err := domain.Transition(from, to); err != nil {
		return fmt.Errorf("UpdateState: %w", err)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("UpdateState: begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	// 2. Update the transaction row — but ONLY if the current state in the DB
	//    matches `from`. The WHERE clause on `state` is the concurrency guard:
	//    if two callers race to transition the same payment, only one wins.
	//    The other sees rowsAffected = 0 and gets a clear error rather than
	//    silently overwriting a state that has already moved on.
	tag, err := tx.Exec(ctx, `
        UPDATE transactions
           SET state      = $1,
               updated_at = now()
         WHERE payment_id = $2
           AND state      = $3
    `, to, paymentID, from)
	if err != nil {
		return fmt.Errorf("UpdateState: update transactions: %w", err)
	}

	// 3. rowsAffected = 0 has two possible causes:
	//    a) payment_id does not exist at all.
	//    b) payment_id exists but its current state != `from` (lost the race,
	//       or caller passed the wrong `from`).
	//    We distinguish them with a follow-up SELECT so we can return a
	//    meaningful error to the caller instead of a generic "not found".
	if tag.RowsAffected() == 0 {
		var currentState domain.State
		err := tx.QueryRow(ctx, `
            SELECT state FROM transactions WHERE payment_id = $1
        `, paymentID).Scan(&currentState)

		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("UpdateState: payment %s not found", paymentID)
		}
		if err != nil {
			return fmt.Errorf("UpdateState: check current state: %w", err)
		}
		// Payment exists but state has already moved — concurrent update won.
		return fmt.Errorf("UpdateState: stale transition %s->%s: current state is %s (payment %s)",
			from, to, currentState, paymentID)
	}

	// 4. Write the audit row. This is unconditional — every state change gets
	//    a record. Gives you a full timeline of every transition for any
	//    payment, which is invaluable during incident investigation.
	_, err = tx.Exec(ctx, `
        INSERT INTO state_transitions (payment_id, from_state, to_state, created_at)
        VALUES ($1, $2, $3, now())
    `, paymentID, from, to)
	if err != nil {
		return fmt.Errorf("UpdateState: insert state_transitions: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("UpdateState: commit: %w", err)
	}

	return nil
}
