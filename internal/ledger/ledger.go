// Package ledger posts balanced double-entry records for captured payments.
package ledger

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/varadsat/distributed-payment-pipeline/internal/domain"
)

// ErrAlreadyPosted is returned when both ledger legs for a payment_id already
// exist. Callers should treat this as success (idempotent re-delivery).
var ErrAlreadyPosted = fmt.Errorf("ledger: payment already posted")

// Poster writes a balanced pair of entries (debit + credit) for a transaction.
// The two legs MUST sum to zero; reject anything that doesn't balance.
type Poster interface {
	Post(ctx context.Context, t domain.Transaction) error
}

// TODO: implement double-entry posting + a balance() assertion in tests.
type DoubleEntryPoster struct {
	pool   *pgxpool.Pool
	logger *slog.Logger
}

func NewDoubleEntryPoster(pool *pgxpool.Pool, logger *slog.Logger) *DoubleEntryPoster {
	return &DoubleEntryPoster{pool: pool, logger: logger}
}

func (p *DoubleEntryPoster) Post(ctx context.Context, trx domain.Transaction) error {
	if trx.Amount.MinorUnits <= 0 {
		return fmt.Errorf("amount must be positive, got %d", trx.Amount.MinorUnits)
	}

	p.logger.Info("posting ledger entries", "payment_id", trx.PaymentID, "amount_minor", trx.Amount.MinorUnits, "currency", trx.Amount.Currency)

	tx, err := p.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback(ctx)
		} else {
			err = tx.Commit(ctx)
		}
	}()

	// Insert debit leg (customer account is debited).
	// ON CONFLICT DO NOTHING makes this idempotent: a duplicate Kafka delivery
	// (relay re-publish or consumer crash before offset commit) is a safe no-op.
	debitTag, err := tx.Exec(ctx, `
		INSERT INTO ledger_entries (payment_id, account, direction, amount_minor, currency)
		VALUES ($1, $2, 'DEBIT', $3, $4)
		ON CONFLICT (payment_id, direction) DO NOTHING`,
		trx.PaymentID,
		trx.AccountID,
		trx.Amount.MinorUnits,
		trx.Amount.Currency,
	)
	if err != nil {
		return fmt.Errorf("insert debit: %w", err)
	}

	// Insert credit leg (clearing account is credited, zero-sum preserved).
	creditTag, err := tx.Exec(ctx, `
		INSERT INTO ledger_entries (payment_id, account, direction, amount_minor, currency)
		VALUES ($1, 'clearing_account', 'CREDIT', $2, $3)
		ON CONFLICT (payment_id, direction) DO NOTHING`,
		trx.PaymentID,
		trx.Amount.MinorUnits,
		trx.Amount.Currency,
	)
	if err != nil {
		return fmt.Errorf("insert credit: %w", err)
	}

	// Both legs already existed — this is a duplicate delivery. Roll back the
	// no-op transaction and return ErrAlreadyPosted so the caller can log it
	// without treating it as a failure.
	if debitTag.RowsAffected() == 0 && creditTag.RowsAffected() == 0 {
		_ = tx.Rollback(ctx)
		err = nil // prevent defer from double-rolling-back
		p.logger.Warn("duplicate ledger post skipped", "payment_id", trx.PaymentID)
		return ErrAlreadyPosted
	}

	p.logger.Info("ledger entries posted", "payment_id", trx.PaymentID)
	return nil
}
