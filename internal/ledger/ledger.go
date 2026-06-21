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
	_, err = tx.Exec(ctx, `
		INSERT INTO ledger_entries (payment_id, account, direction, amount_minor, currency)
		VALUES ($1, $2, 'DEBIT', $3, $4)`,
		trx.PaymentID,
		trx.AccountID,
		trx.Amount.MinorUnits,
		trx.Amount.Currency,
	)
	if err != nil {
		return fmt.Errorf("insert debit: %w", err)
	}

	// Insert credit leg (clearing account is credited, zero-sum preserved).
	_, err = tx.Exec(ctx, `
		INSERT INTO ledger_entries (payment_id, account, direction, amount_minor, currency)
		VALUES ($1, 'clearing_account', 'CREDIT', $2, $3)`,
		trx.PaymentID,
		trx.Amount.MinorUnits,
		trx.Amount.Currency,
	)
	if err != nil {
		return fmt.Errorf("insert credit: %w", err)
	}

	p.logger.Info("ledger entries posted", "payment_id", trx.PaymentID)
	return nil
}
