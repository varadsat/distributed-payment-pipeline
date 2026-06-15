// Package normalize converts per-source payloads into a canonical Transaction.
package normalize

import "github.com/yourname/payment-pipeline/internal/domain"

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

// TODO: implement Register / Get and concrete normalizers for CARD, UPI, etc.
