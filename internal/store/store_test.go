package store

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/varadsat/distributed-payment-pipeline/internal/domain"
)

func newTestStore(t *testing.T) *pgxStore {
	t.Helper()

	ctx := context.Background()

	postgresContainer, err := postgres.Run(
		ctx,
		"postgres:16-alpine",
		postgres.WithInitScripts("migrations/0001_init.up.sql"),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).
			WithStartupTimeout(60*time.Second)),
	)

	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}

	t.Cleanup(func() {
		_ = postgresContainer.Terminate(ctx)
	})

	connStr, err := postgresContainer.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}

	connStr = fmt.Sprintf("%s&sslmode=disable", connStr)

	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		t.Fatalf("create pool: %v", err)
	}

	t.Cleanup(func() {
		pool.Close()
	})

	return &pgxStore{pool: pool}
}
func TestSaveAndRetrieveTransaction(t *testing.T) {
	ctx := context.Background()

	s := newTestStore(t) // helper that creates a real store

	tx := domain.Transaction{
		PaymentID:      "11111111-1111-1111-1111-111111111111",
		IdempotencyKey: "idem-123",
		Source:         domain.SourceCard,
		ExternalTxnID:  "ext-123",
		AccountID:      "acct-1",
		Amount: domain.Money{
			MinorUnits: 2500,
			Currency:   "INR",
		},
		State:         domain.StateReceived,
		SchemaVersion: 1,
		Metadata: map[string]string{
			"note": "test",
		},
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}

	if err := s.Save(ctx, tx); err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	gotByID, err := s.GetByPaymentID(ctx, tx.PaymentID)
	if err != nil {
		t.Fatalf("GetByPaymentID() error = %v", err)
	}

	if gotByID.PaymentID != tx.PaymentID {
		t.Fatalf("PaymentID mismatch: got %q want %q", gotByID.PaymentID, tx.PaymentID)
	}
	if gotByID.IdempotencyKey != tx.IdempotencyKey {
		t.Fatalf("IdempotencyKey mismatch: got %q want %q", gotByID.IdempotencyKey, tx.IdempotencyKey)
	}
	if gotByID.Amount != tx.Amount {
		t.Fatalf("Amount mismatch: got %+v want %+v", gotByID.Amount, tx.Amount)
	}

	gotByKey, err := s.GetByIdempotencyKey(ctx, tx.IdempotencyKey)
	if err != nil {
		t.Fatalf("GetByIdempotencyKey() error = %v", err)
	}

	if gotByKey.PaymentID != tx.PaymentID {
		t.Fatalf("PaymentID mismatch via idempotency key: got %q want %q", gotByKey.PaymentID, tx.PaymentID)
	}
}
