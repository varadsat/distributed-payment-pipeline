// internal/outbox/event.go
package outbox

import (
	"time"

	"github.com/google/uuid"
	"github.com/varadsat/distributed-payment-pipeline/internal/domain"
)

// PaymentReceivedEvent is the payload published to payments.received.
// This is the contract every downstream consumer depends on — version it
// deliberately if it ever needs to change shape.
type PaymentReceivedEvent struct {
	EventID       string `json:"event_id"`       // unique per outbox row, for consumer dedup
	EventType     string `json:"event_type"`     // "payment.received"
	SchemaVersion int    `json:"schema_version"` // event schema version, separate from intake's

	PaymentID      string `json:"payment_id"`
	IdempotencyKey string `json:"idempotency_key"`
	Source         string `json:"source"`
	ExternalTxnID  string `json:"external_txn_id,omitempty"`
	AccountID      string `json:"account_id"`

	AmountMinor int64  `json:"amount_minor"`
	Currency    string `json:"currency"`

	State     string    `json:"state"` // RECEIVED at this point
	CreatedAt time.Time `json:"created_at"`

	Metadata map[string]string `json:"metadata,omitempty"`
}

func NewPaymentReceivedEvent(internalTx domain.Transaction) PaymentReceivedEvent {
	return PaymentReceivedEvent{
		EventID:        uuid.New().String(),
		EventType:      "payment.received",
		SchemaVersion:  1,
		PaymentID:      internalTx.PaymentID,
		IdempotencyKey: internalTx.IdempotencyKey,
		Source:         string(internalTx.Source),
		ExternalTxnID:  internalTx.ExternalTxnID,
		AccountID:      internalTx.AccountID,
		AmountMinor:    internalTx.Amount.MinorUnits,
		Currency:       internalTx.Amount.Currency,
		State:          string(internalTx.State),
		CreatedAt:      internalTx.CreatedAt,
		Metadata:       internalTx.Metadata,
	}
}
