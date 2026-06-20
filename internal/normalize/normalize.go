// Package normalize converts per-source payloads into a canonical Transaction.
package normalize

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/varadsat/distributed-payment-pipeline/internal/domain"
)

// SourceNormalizer maps one source's raw payload into the canonical form.
// Register one per (source, schema_version) pair to support schema evolution.
type SourceNormalizer interface {
	Normalize(raw map[string]string) (domain.Transaction, error)
}

// Registry selects the right normalizer for a given source + schema version.
type Registry struct {
	// key: source + ":" + schemaVersion
	handlers map[string]SourceNormalizer
}

func (r *Registry) Get(source string, schemaVersion int32) (SourceNormalizer, error) {
	key := fmt.Sprintf("%s:%d", source, schemaVersion)
	normalizer, exists := r.handlers[key]
	if !exists {
		return nil, fmt.Errorf("no normalizer found for source %s with schema version %d", source, schemaVersion)
	}
	return normalizer, nil
}

func (r *Registry) Register(source string, schemaVersion int32, normalizer SourceNormalizer) {
	key := fmt.Sprintf("%s:%d", source, schemaVersion)
	r.handlers[key] = normalizer
}

func NewRegistry() *Registry {
	return &Registry{
		handlers: make(map[string]SourceNormalizer),
	}
}

// TODO: implement Register / Get and concrete normalizers for CARD, UPI, etc.

type CardNormalizer struct{}

func (c *CardNormalizer) Normalize(raw map[string]string) (domain.Transaction, error) {
	// Implement normalization logic for CARD source here

	source := domain.SourceCard
	accountID := raw["account_id"]
	// Convert amount to domain.Money
	minorUnits, err := parseAmount(raw["amount"])
	if err != nil {
		return domain.Transaction{}, fmt.Errorf("invalid amount: %v", err)
	}
	amount := domain.Money{
		MinorUnits: minorUnits,
		Currency:   raw["currency"],
	}

	transaction := domain.Transaction{
		PaymentID:      raw["payment_id"],
		IdempotencyKey: raw["idempotency_key"],
		Source:         source,
		ExternalTxnID:  raw["external_txn_id"],
		AccountID:      accountID,
		Amount:         amount,
		State:          domain.State(raw["state"]),
		SchemaVersion:  1,
		Metadata:       raw, // Store the raw data as metadata for reference
	}

	return transaction, nil
}

func parseAmount(amount string) (int64, error) {
	amount = strings.TrimSpace(amount)
	if amount == "" {
		return 0, fmt.Errorf("amount is empty")
	}

	negative := false
	if strings.HasPrefix(amount, "-") {
		negative = true
		amount = strings.TrimPrefix(amount, "-")
	}

	parts := strings.Split(amount, ".")
	if len(parts) > 2 {
		return 0, fmt.Errorf("invalid amount format")
	}

	major := parts[0]
	minor := ""
	if len(parts) == 2 {
		minor = parts[1]
	}

	if major == "" {
		major = "0"
	}

	if len(minor) > 2 {
		return 0, fmt.Errorf("invalid amount precision")
	}
	for len(minor) < 2 {
		minor += "0"
	}

	majorUnits, err := strconv.ParseInt(major, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid amount major units: %w", err)
	}
	minorUnits, err := strconv.ParseInt(minor, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid amount minor units: %w", err)
	}

	total := majorUnits*100 + minorUnits
	if negative {
		total = -total
	}
	return total, nil
}

type UPINormalizer struct{}

func (u *UPINormalizer) Normalize(raw map[string]string) (domain.Transaction, error) {
	// Implement normalization logic for UPI source here

	source := domain.SourceUPI
	accountID := raw["account_id"]
	// Convert amount to domain.Money
	minorUnits, err := parseAmount(raw["amount"])
	if err != nil {
		return domain.Transaction{}, fmt.Errorf("invalid amount: %v", err)
	}
	amount := domain.Money{
		MinorUnits: minorUnits,
		Currency:   raw["currency"],
	}

	transaction := domain.Transaction{
		PaymentID:      raw["payment_id"],
		IdempotencyKey: raw["idempotency_key"],
		Source:         source,
		ExternalTxnID:  raw["upi_trx_id"],
		AccountID:      accountID,
		Amount:         amount,
		State:          domain.State(raw["state"]),
		SchemaVersion:  1,
		Metadata:       raw, // Store the raw data as metadata for reference
	}

	return transaction, nil
}
