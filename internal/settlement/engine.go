// Package settlement batches CAPTURED transactions per account over a window
// and emits settlement/payout events.
package settlement

import "context"

// Engine aggregates captured transactions per account over a settlement window
// (e.g. hourly), writes a settlement_batches row, and emits payouts.requested.
type Engine interface {
	Run(ctx context.Context) error
}

// TODO: window accumulation, batch persistence, payout event emission, reconcile.
