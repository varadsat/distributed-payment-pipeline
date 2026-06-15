// Package domain holds the canonical, transport-agnostic types. Nothing here
// imports gRPC, Kafka, or SQL — those live in adapter packages.
package domain

import "time"

// Money is always in minor units. Never use float for money.
type Money struct {
	MinorUnits int64
	Currency   string // ISO 4217
}

type Source string

const (
	SourceCard         Source = "CARD"
	SourceUPI          Source = "UPI"
	SourceBankTransfer Source = "BANK_TRANSFER"
	SourceWallet       Source = "WALLET"
)

// Transaction is the canonical internal representation produced by the
// normalizer, regardless of which source the payment arrived from.
type Transaction struct {
	PaymentID      string
	IdempotencyKey string
	Source         Source
	ExternalTxnID  string
	AccountID      string
	Amount         Money
	State          State
	SchemaVersion  uint32
	Metadata       map[string]string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}
