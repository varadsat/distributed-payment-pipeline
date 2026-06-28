package ledger

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/varadsat/distributed-payment-pipeline/internal/domain"
	"github.com/varadsat/distributed-payment-pipeline/internal/store"
)

func newTestPoster(t *testing.T) (*DoubleEntryPoster, *pgxpool.Pool, store.Store) {
	t.Helper()

	ctx := context.Background()

	pg, err := postgres.Run(
		ctx,
		"postgres:16-alpine",
		postgres.WithInitScripts(
			"../store/migrations/0001_init.up.sql",
			"../store/migrations/0002_ledger_idempotency.up.sql",
		),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}

	t.Cleanup(func() { _ = pg.Terminate(ctx) })

	connStr, err := pg.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}
	connStr = fmt.Sprintf("%s&sslmode=disable", connStr)

	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		t.Fatalf("create pool: %v", err)
	}
	t.Cleanup(pool.Close)

	st, err := store.NewStore(ctx, connStr)
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	t.Cleanup(st.Close)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return NewDoubleEntryPoster(pool, st, logger), pool, st
}

func newTrx() domain.Transaction {
	return domain.Transaction{
		PaymentID:      uuid.New().String(),
		IdempotencyKey: uuid.New().String(),
		AccountID:      "acct-001",
		Amount:         domain.Money{MinorUnits: 5000, Currency: "INR"},
		Source:         domain.SourceCard,
		State:          domain.StateValidated,
		SchemaVersion:  1,
		CreatedAt:      time.Now().UTC(),
		UpdatedAt:      time.Now().UTC(),
	}
}

// TestPost_InsertsDebitAndCreditLegs verifies that Post writes exactly one
// DEBIT and one CREDIT row and that both amounts match the transaction.
func TestPost_InsertsDebitAndCreditLegs(t *testing.T) {
	poster, pool, st := newTestPoster(t)
	ctx := context.Background()
	trx := newTrx()

	// Insert the transaction in VALIDATED state before posting
	if err := st.Save(ctx, trx); err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	if err := poster.Post(ctx, trx); err != nil {
		t.Fatalf("Post() error = %v", err)
	}

	type row struct {
		account     string
		direction   string
		amountMinor int64
		currency    string
	}

	rows, err := pool.Query(ctx,
		`SELECT account, direction, amount_minor, currency
		 FROM ledger_entries
		 WHERE payment_id = $1
		 ORDER BY direction`, trx.PaymentID)
	if err != nil {
		t.Fatalf("query ledger_entries: %v", err)
	}
	defer rows.Close()

	var entries []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.account, &r.direction, &r.amountMinor, &r.currency); err != nil {
			t.Fatalf("scan: %v", err)
		}
		entries = append(entries, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}

	if len(entries) != 2 {
		t.Fatalf("ledger_entries count = %d, want 2", len(entries))
	}

	for _, e := range entries {
		if e.amountMinor != trx.Amount.MinorUnits {
			t.Errorf("amount_minor = %d, want %d (direction=%s)", e.amountMinor, trx.Amount.MinorUnits, e.direction)
		}
		if e.currency != trx.Amount.Currency {
			t.Errorf("currency = %q, want %q (direction=%s)", e.currency, trx.Amount.Currency, e.direction)
		}
	}

	// CREDIT row is first after ORDER BY direction; DEBIT second.
	if entries[0].direction != "CREDIT" || entries[0].account != "clearing_account" {
		t.Errorf("first row = {direction:%q account:%q}, want {CREDIT clearing_account}", entries[0].direction, entries[0].account)
	}
	if entries[1].direction != "DEBIT" || entries[1].account != trx.AccountID {
		t.Errorf("second row = {direction:%q account:%q}, want {DEBIT %s}", entries[1].direction, entries[1].account, trx.AccountID)
	}

	// Verify state transition to CAPTURED
	updatedTrx, err := st.GetByPaymentID(ctx, trx.PaymentID)
	if err != nil {
		t.Fatalf("GetByPaymentID() error = %v", err)
	}
	if updatedTrx.State != domain.StateCaptured {
		t.Errorf("state = %q, want %q", updatedTrx.State, domain.StateCaptured)
	}
}

// TestPost_DuplicateDeliveryReturnsErrAlreadyPosted verifies that a second
// Post() for the same payment_id returns ErrAlreadyPosted (idempotent skip)
// rather than inserting duplicate ledger legs.
func TestPost_DuplicateDeliveryReturnsErrAlreadyPosted(t *testing.T) {
	poster, pool, st := newTestPoster(t)
	ctx := context.Background()
	trx := newTrx()

	// Insert the transaction in VALIDATED state before posting
	if err := st.Save(ctx, trx); err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	if err := poster.Post(ctx, trx); err != nil {
		t.Fatalf("first Post() error = %v", err)
	}

	// Simulate at-least-once re-delivery of the same Kafka record.
	if err := poster.Post(ctx, trx); err != ErrAlreadyPosted {
		t.Fatalf("second Post() = %v, want ErrAlreadyPosted", err)
	}

	// Confirm no duplicate rows were inserted — still exactly 2.
	var count int
	if err := pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM ledger_entries WHERE payment_id = $1`, trx.PaymentID,
	).Scan(&count); err != nil {
		t.Fatalf("count query: %v", err)
	}
	if count != 2 {
		t.Errorf("ledger_entries count = %d after duplicate delivery, want 2", count)
	}
}

// TestPost_ZeroAmountIsRejected verifies that a zero-value payment results in
// an error rather than writing silent no-op entries.
func TestPost_ZeroAmountIsRejected(t *testing.T) {
	poster, _, _ := newTestPoster(t)
	ctx := context.Background()

	trx := newTrx()
	trx.Amount.MinorUnits = 0

	err := poster.Post(ctx, trx)
	if err == nil {
		t.Fatal("Post() with zero amount should return an error, got nil")
	}
}
