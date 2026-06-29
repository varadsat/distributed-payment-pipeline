package settlement

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// pgxSettlementStore implements settlement.Store using pgx.
// Wire this in cmd/settlement/main.go.
type pgxSettlementStore struct {
	pool *pgxpool.Pool
}

func NewPgxStore(pool *pgxpool.Pool) Store {
	return &pgxSettlementStore{pool: pool}
}

// AggregateCaptured executes the GROUP BY query over the CAPTURED window.
//
// The ARRAY_AGG(payment_id) collects the individual payment IDs so
// PersistBatch can bulk-update their state without a second query.
//
// captured_at is indexed with a partial index WHERE state = 'CAPTURED'
// (migration 0002), so this scan is cheap even on large tables.
func (s *pgxSettlementStore) AggregateCaptured(
	ctx context.Context,
	windowStart, windowEnd time.Time,
) ([]CapturedGroup, error) {
	const q = `
		SELECT
			account_id,
			currency,
			SUM(amount_minor)            AS total_minor,
			COUNT(*)                     AS txn_count,
			ARRAY_AGG(payment_id::text)  AS payment_ids
		FROM transactions
		WHERE state       = 'CAPTURED'
		  AND created_at >= $1
		  AND created_at <  $2
		GROUP BY account_id, currency
		ORDER BY account_id, currency  -- deterministic order for tests
	`
	//TODO: created_at to be replaced with captured_at
	rows, err := s.pool.Query(ctx, q, windowStart, windowEnd)
	if err != nil {
		return nil, fmt.Errorf("aggregate query: %w", err)
	}
	defer rows.Close()

	var groups []CapturedGroup
	for rows.Next() {
		var g CapturedGroup
		if err := rows.Scan(
			&g.AccountID,
			&g.Currency,
			&g.TotalMinor,
			&g.TxnCount,
			&g.PaymentIDs,
		); err != nil {
			return nil, fmt.Errorf("scan group row: %w", err)
		}
		groups = append(groups, g)
	}
	return groups, rows.Err()
}

// PersistBatch runs three writes inside a single DB transaction:
//
//  1. INSERT INTO settlement_batches ... ON CONFLICT DO NOTHING
//     Returns alreadyExisted=true if the unique constraint fires.
//
//  2. Bulk UPDATE transactions SET state='SETTLED' WHERE payment_id = ANY(ids)
//     Only the IDs in the group are touched — other accounts/currencies are safe.
//
//  3. INSERT two outbox rows (payments.settled, payouts.requested) so the
//     relay publishes them without a dual-write.
//
// If any step fails the whole transaction is rolled back — no partial state.
func (s *pgxSettlementStore) PersistBatch(
	ctx context.Context,
	batch Batch,
	group CapturedGroup,
	settledPayload, payoutPayload []byte,
) (alreadyExisted bool, err error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin tx: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback(ctx)
		}
	}()

	// --- 1. Insert batch row (idempotent via ON CONFLICT DO NOTHING) ---
	const insertBatch = `
		INSERT INTO settlement_batches
			(id, account_id, currency, window_start, window_end,
			 total_minor, txn_count, status, created_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		ON CONFLICT (account_id, currency, window_start, window_end)
		DO NOTHING
	`
	tag, err := tx.Exec(ctx, insertBatch,
		batch.ID, batch.AccountID, batch.Currency,
		batch.WindowStart, batch.WindowEnd,
		batch.TotalMinor, batch.TxnCount,
		string(batch.Status), batch.CreatedAt,
	)
	if err != nil {
		return false, fmt.Errorf("insert settlement_batch: %w", err)
	}
	if tag.RowsAffected() == 0 {
		// ON CONFLICT fired — this window was already settled. Roll back
		// (nothing was written) and tell the engine to skip.
		_ = tx.Rollback(ctx)
		return true, nil
	}

	// --- 2. Bulk-update transaction states to SETTLED ---
	//
	// pgx accepts []string for ANY($1). The WHERE clause also checks that
	// state is still CAPTURED — belt-and-suspenders guard against a race
	// where a concurrent run already moved one of these payments.
	const updateTxns = `
		UPDATE transactions
		SET    state      = 'SETTLED',
		       updated_at = now()
		WHERE  payment_id = ANY($1)
		  AND  state      = 'CAPTURED'
	`
	if _, err = tx.Exec(ctx, updateTxns, group.PaymentIDs); err != nil {
		return false, fmt.Errorf("bulk update transactions: %w", err)
	}

	// --- 3. Write outbox rows (two — one per topic) ---
	//
	// Partition key is account_id on both topics so the relay preserves
	// per-account event ordering (same discipline as payments.received).
	if err = insertOutboxRow(ctx, tx, batch.ID, "payments.settled",
		batch.AccountID, settledPayload); err != nil {
		return false, fmt.Errorf("outbox payments.settled: %w", err)
	}
	if err = insertOutboxRow(ctx, tx, batch.ID, "payouts.requested",
		batch.AccountID, payoutPayload); err != nil {
		return false, fmt.Errorf("outbox payouts.requested: %w", err)
	}

	if err = tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit: %w", err)
	}
	return false, nil
}

// insertOutboxRow writes a single row to the outbox table inside an existing
// transaction. The relay will pick it up and publish to Kafka.
func insertOutboxRow(
	ctx context.Context,
	tx pgx.Tx,
	aggregateID, topic, partitionKey string,
	payload []byte,
) error {
	const q = `
		INSERT INTO outbox (aggregate_id, topic, partition_key, payload)
		VALUES ($1, $2, $3, $4)
	`
	_, err := tx.Exec(ctx, q, aggregateID, topic, partitionKey, payload)
	return err
}

// errDuplicateBatch is returned when the unique constraint fires and we detect
// it via error inspection rather than RowsAffected (defensive path).
var errDuplicateBatch = errors.New("settlement batch already exists for this window")
