// Package ledger posts balanced double-entry records for captured payments.
package ledger

import "github.com/yourname/payment-pipeline/internal/domain"

// Poster writes a balanced pair of entries (debit + credit) for a transaction.
// The two legs MUST sum to zero; reject anything that doesn't balance.
type Poster interface {
	Post(t domain.Transaction) error
}

// TODO: implement double-entry posting + a balance() assertion in tests.
