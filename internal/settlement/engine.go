// Package settlement batches CAPTURED transactions per (account_id, currency)
// over a fixed time window and emits two Kafka events via the transactional
// outbox: payments.settled (for downstream consumers like ledger/analytics)
// and payouts.requested (for a bank integration or stub reconciler).
package settlement

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
)

// Engine is the public interface. cmd/settlement calls Run in a loop.
type Engine interface {
	Run(ctx context.Context) error
}

// Config controls the engine's timing behaviour.
// All values should be loaded from env via internal/config.
type Config struct {
	// WindowSize is how long each settlement window covers.
	// e.g. 1*time.Hour means "settle everything captured in the last full hour."
	WindowSize time.Duration

	// Buffer is subtracted from now() to form window_end.
	// Guards against in-flight transactions that were CAPTURED moments ago
	// and may not yet be committed/visible (replication lag, clock skew).
	// e.g. 5*time.Minute means window_end = now() - 5m.
	Buffer time.Duration

	// Interval is how often the engine wakes up to check for a new window.
	// Should be ≤ WindowSize. e.g. 1*time.Minute checks every minute,
	// but only opens a new batch when a full WindowSize has elapsed.
	Interval time.Duration
}

// pgEngine is the concrete Engine implementation.
type pgEngine struct {
	store  Store
	cfg    Config
	logger *slog.Logger

	// lastWindowEnd tracks the end of the most recently processed window so
	// the engine knows where the next window should start. Zero value means
	// "no window processed yet — derive start from the oldest CAPTURED txn"
	// or simply start from now()-WindowSize on first run.
	lastWindowEnd time.Time
}

// New constructs an Engine. Inject a real pgxSettlementStore for production
// and a fakeStore for unit tests.
func New(store Store, cfg Config, logger *slog.Logger) Engine {
	return &pgEngine{
		store:  store,
		cfg:    cfg,
		logger: logger,
	}
}

// Run is the main loop. It ticks on cfg.Interval and, when a full window has
// elapsed since the last processed window, triggers a settlement run.
// Blocks until ctx is cancelled — call it in a goroutine from cmd/settlement.
func (e *pgEngine) Run(ctx context.Context) error {
	ticker := time.NewTicker(e.cfg.Interval)
	defer ticker.Stop()

	e.logger.Info("settlement engine started",
		"window_size", e.cfg.WindowSize,
		"buffer", e.cfg.Buffer,
		"interval", e.cfg.Interval,
	)

	for {
		select {
		case <-ctx.Done():
			e.logger.Info("settlement engine stopping", "reason", ctx.Err())
			return ctx.Err()

		case tick := <-ticker.C:
			windowEnd := tick.Add(-e.cfg.Buffer) // exclude the buffer tail

			// Only open a new window when a full WindowSize has elapsed.
			if e.lastWindowEnd.IsZero() {
				// First tick: set lastWindowEnd so the first window is
				// [now()-WindowSize-Buffer, now()-Buffer].
				e.lastWindowEnd = windowEnd.Add(-e.cfg.WindowSize)
			}

			windowStart := e.lastWindowEnd
			if windowEnd.Sub(windowStart) < e.cfg.WindowSize {
				// Not enough time has elapsed yet — wait for the next tick.
				continue
			}

			if err := e.settle(ctx, windowStart, windowEnd); err != nil {
				// Log and continue — a single failed window should not
				// crash the engine. The next tick will retry (idempotency
				// on the DB side means no double-settling risk).
				e.logger.Error("settlement run failed",
					"window_start", windowStart,
					"window_end", windowEnd,
					"err", err,
				)
				continue
			}

			e.lastWindowEnd = windowEnd
		}
	}
}

// settle processes a single window: aggregate → persist batch → outbox events.
// This is the unit-testable core of the engine. Tests call it directly.
func (e *pgEngine) settle(ctx context.Context, windowStart, windowEnd time.Time) error {
	log := e.logger.With("window_start", windowStart, "window_end", windowEnd)
	log.Info("settlement window opened")

	// -----------------------------------------------------------------------
	// Step 1: Aggregate all CAPTURED transactions in the window.
	//
	// Returns one CapturedGroup per (account_id, currency). The SQL behind
	// this is a GROUP BY with SUM(amount_minor) and ARRAY_AGG(payment_id).
	// -----------------------------------------------------------------------
	groups, err := e.store.AggregateCaptured(ctx, windowStart, windowEnd)
	if err != nil {
		return fmt.Errorf("aggregate captured: %w", err)
	}

	if len(groups) == 0 {
		log.Info("no captured transactions in window — nothing to settle")
		return nil
	}

	log.Info("aggregated captured groups", "group_count", len(groups))

	// -----------------------------------------------------------------------
	// Step 2: For each (account_id, currency) group, persist a batch and
	// emit two Kafka events via the outbox (in one DB transaction).
	// -----------------------------------------------------------------------
	var settled, skipped int

	for _, group := range groups {
		if err := e.processGroup(ctx, group, windowStart, windowEnd); err != nil {
			// Log per-group errors but continue — a failure on one account
			// should not block settlement for others.
			log.Error("failed to process group",
				"account_id", group.AccountID,
				"currency", group.Currency,
				"err", err,
			)
			continue
		}
		settled++
	}

	log.Info("settlement window complete",
		"settled_groups", settled,
		"skipped_groups", skipped,
	)
	return nil
}

// processGroup handles one (account_id, currency) group:
//  1. Build the Batch domain object.
//  2. Encode the two outbox payloads (SettledEvent, PayoutRequestedEvent).
//  3. Call store.PersistBatch — atomically inserts the batch row, bulk-updates
//     transaction states to SETTLED, and writes both outbox rows.
//     Returns alreadyExisted=true if the unique constraint fired (idempotent).
func (e *pgEngine) processGroup(
	ctx context.Context,
	group CapturedGroup,
	windowStart, windowEnd time.Time,
) error {
	batchID := uuid.New().String()
	payoutID := uuid.New().String()
	now := time.Now().UTC()

	batch := Batch{
		ID:          batchID,
		AccountID:   group.AccountID,
		Currency:    group.Currency,
		WindowStart: windowStart,
		WindowEnd:   windowEnd,
		TotalMinor:  group.TotalMinor,
		TxnCount:    group.TxnCount,
		Status:      BatchStatusPending,
		CreatedAt:   now,
	}

	// --- encode outbox payloads ---

	settledPayload, err := json.Marshal(SettledEvent{
		BatchID:     batchID,
		AccountID:   group.AccountID,
		Currency:    group.Currency,
		WindowStart: windowStart,
		WindowEnd:   windowEnd,
		TotalMinor:  group.TotalMinor,
		TxnCount:    group.TxnCount,
	})
	if err != nil {
		return fmt.Errorf("marshal settled event: %w", err)
	}

	payoutPayload, err := json.Marshal(PayoutRequestedEvent{
		PayoutID:    payoutID,
		BatchID:     batchID,
		AccountID:   group.AccountID,
		Currency:    group.Currency,
		TotalMinor:  group.TotalMinor,
		RequestedAt: now,
	})
	if err != nil {
		return fmt.Errorf("marshal payout event: %w", err)
	}

	// --- atomic: insert batch + update txn states + write outbox rows ---

	alreadyExisted, err := e.store.PersistBatch(ctx, batch, group, settledPayload, payoutPayload)
	if err != nil {
		return fmt.Errorf("persist batch (account=%s currency=%s): %w",
			group.AccountID, group.Currency, err)
	}

	if alreadyExisted {
		// Duplicate run for this window — the unique constraint fired.
		// This is expected if the engine restarted mid-run. Safe to skip.
		e.logger.Info("batch already exists — skipping (idempotent)",
			"account_id", group.AccountID,
			"currency", group.Currency,
			"window_start", windowStart,
		)
		return nil
	}

	e.logger.Info("batch persisted",
		"batch_id", batchID,
		"account_id", group.AccountID,
		"currency", group.Currency,
		"total_minor", group.TotalMinor,
		"txn_count", group.TxnCount,
	)
	return nil
}
