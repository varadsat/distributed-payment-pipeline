package settlement

import "time"

// BatchStatus is the lifecycle of a settlement batch.
type BatchStatus string

const (
	BatchStatusPending    BatchStatus = "PENDING"     // computed, not yet paid out
	BatchStatusPayoutSent BatchStatus = "PAYOUT_SENT" // payout instruction emitted
	BatchStatusReconciled BatchStatus = "RECONCILED"  // bank confirmed the payout
)

// Batch is the domain type for a settlement batch — one row in settlement_batches.
// It covers all CAPTURED transactions for a single (account_id, currency) pair
// inside a fixed time window.
type Batch struct {
	ID          string
	AccountID   string
	Currency    string // ISO 4217
	WindowStart time.Time
	WindowEnd   time.Time
	TotalMinor  int64 // sum of amount_minor for all transactions in the batch
	TxnCount    int
	Status      BatchStatus
	CreatedAt   time.Time
}

// CapturedGroup is the result of the aggregation query: one row per
// (account_id, currency) representing all CAPTURED txns in the window.
type CapturedGroup struct {
	AccountID  string
	Currency   string
	TotalMinor int64
	TxnCount   int
	PaymentIDs []string // used to bulk-update state to SETTLED in the same tx
}

// SettledEvent is published to payments.settled after a batch is persisted.
// Downstream consumers (ledger, analytics) react to this.
type SettledEvent struct {
	BatchID     string    `json:"batch_id"`
	AccountID   string    `json:"account_id"`
	Currency    string    `json:"currency"`
	WindowStart time.Time `json:"window_start"`
	WindowEnd   time.Time `json:"window_end"`
	TotalMinor  int64     `json:"total_minor"`
	TxnCount    int       `json:"txn_count"`
}

// PayoutRequestedEvent is published to payouts.requested. In production this
// drives a bank integration. Here it is consumed by a stub reconciler.
type PayoutRequestedEvent struct {
	PayoutID    string    `json:"payout_id"`
	BatchID     string    `json:"batch_id"`
	AccountID   string    `json:"account_id"`
	Currency    string    `json:"currency"`
	TotalMinor  int64     `json:"total_minor"`
	RequestedAt time.Time `json:"requested_at"`
}
