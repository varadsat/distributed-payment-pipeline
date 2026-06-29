package settlement

import (
	"context"
	"time"
)

// Store is the persistence contract the engine depends on.
// It is intentionally narrow — only the queries settlement needs.
// The pgx implementation lives in internal/store/settlement_store.go.
type Store interface {
	// AggregateCaptured returns one CapturedGroup per (account_id, currency)
	// for all CAPTURED transactions whose captured_at falls inside the window.
	// This is a read-only aggregation query — no state changes here.
	AggregateCaptured(ctx context.Context, windowStart, windowEnd time.Time) ([]CapturedGroup, error)

	// PersistBatch runs the following atomically in a single DB transaction:
	//   1. INSERT INTO settlement_batches ... ON CONFLICT DO NOTHING
	//      (idempotency guard — duplicate run for same window is a no-op)
	//   2. UPDATE transactions SET state = 'SETTLED', updated_at = now()
	//      WHERE payment_id = ANY(group.PaymentIDs)
	//   3. INSERT two outbox rows (one per Kafka topic) so the relay publishes
	//      payments.settled and payouts.requested without a dual-write.
	//
	// Returns (batch, alreadyExisted, error).
	// alreadyExisted=true means the unique constraint fired — safe to skip.
	PersistBatch(ctx context.Context, batch Batch, group CapturedGroup,
		settledPayload, payoutPayload []byte) (alreadyExisted bool, err error)
}
