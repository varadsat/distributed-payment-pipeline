// Package ledger posts balanced double-entry records for captured payments.
package ledger

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/varadsat/distributed-payment-pipeline/internal/domain"
	"github.com/varadsat/distributed-payment-pipeline/internal/store"
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
	store  store.Store
	logger *slog.Logger
}

func NewDoubleEntryPoster(pool *pgxpool.Pool, st store.Store, logger *slog.Logger) *DoubleEntryPoster {
	return &DoubleEntryPoster{pool: pool, store: st, logger: logger}
}

func (p *DoubleEntryPoster) Post(ctx context.Context, trx domain.Transaction) error {
	if trx.Amount.MinorUnits <= 0 {
		return fmt.Errorf("amount must be positive, got %d", trx.Amount.MinorUnits)
	}

	p.logger.Info("posting ledger entries", "payment_id", trx.PaymentID, "amount_minor", trx.Amount.MinorUnits, "currency", trx.Amount.Currency)

	tx, err := p.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		// Failed to begin transaction - transition to FAILED state
		if stateErr := p.store.UpdateState(ctx, trx.PaymentID, domain.StateValidated, domain.StateFailed); stateErr != nil {
			p.logger.Error("failed to update state to FAILED after begin error", "payment_id", trx.PaymentID, "error", stateErr)
		}
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
		// Failed to insert debit - transition to FAILED state
		if stateErr := p.store.UpdateState(ctx, trx.PaymentID, domain.StateValidated, domain.StateFailed); stateErr != nil {
			p.logger.Error("failed to update state to FAILED after debit error", "payment_id", trx.PaymentID, "error", stateErr)
		}
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
		// Failed to insert credit - transition to FAILED state
		if stateErr := p.store.UpdateState(ctx, trx.PaymentID, domain.StateValidated, domain.StateFailed); stateErr != nil {
			p.logger.Error("failed to update state to FAILED after credit error", "payment_id", trx.PaymentID, "error", stateErr)
		}
		return fmt.Errorf("insert credit: %w", err)
	}

	// Both legs already existed — this is a duplicate delivery. Roll back the
	// no-op transaction but continue with state transition.
	if debitTag.RowsAffected() == 0 && creditTag.RowsAffected() == 0 {
		_ = tx.Rollback(ctx)
		err = nil // prevent defer from double-rolling-back
		p.logger.Warn("duplicate ledger post skipped", "payment_id", trx.PaymentID)
		// Continue to attempt state transition (idempotent re-delivery).
	} else {
		p.logger.Info("ledger entries posted", "payment_id", trx.PaymentID)
	}

	// Transition state from VALIDATED to CAPTURED after successful posting.
	// On re-delivery, state might already be CAPTURED; that's an acceptable no-op.
	if err := p.store.UpdateState(ctx, trx.PaymentID, domain.StateValidated, domain.StateCaptured); err != nil {
		// If already in CAPTURED state (stale transition), treat as idempotent success.
		if strings.Contains(err.Error(), "stale transition") && strings.Contains(err.Error(), string(domain.StateCaptured)) {
			p.logger.Debug("state already transitioned to CAPTURED", "payment_id", trx.PaymentID)
			return nil
		}
		p.logger.Error("failed to update state to CAPTURED", "payment_id", trx.PaymentID, "error", err)
		return fmt.Errorf("update state to CAPTURED: %w", err)
	}

	p.logger.Info("state transitioned to CAPTURED", "payment_id", trx.PaymentID)
	return nil
}
