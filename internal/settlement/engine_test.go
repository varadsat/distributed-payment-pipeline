package settlement

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"testing"
	"time"
)

// fakeStore is an in-memory settlement.Store for unit tests.
// No DB needed — inject this into pgEngine via New().
type fakeStore struct {
	mu sync.Mutex

	// groups returned by AggregateCaptured
	groups []CapturedGroup

	// batches recorded by PersistBatch calls
	batches []Batch

	// outbox payloads recorded by PersistBatch calls, keyed by topic
	outbox map[string][]json.RawMessage

	// simulateDuplicate makes PersistBatch return alreadyExisted=true
	simulateDuplicate bool
}

func newFakeStore(groups []CapturedGroup) *fakeStore {
	return &fakeStore{
		groups: groups,
		outbox: map[string][]json.RawMessage{
			"payments.settled":  {},
			"payouts.requested": {},
		},
	}
}

func (f *fakeStore) AggregateCaptured(_ context.Context, _, _ time.Time) ([]CapturedGroup, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.groups, nil
}

func (f *fakeStore) PersistBatch(
	_ context.Context,
	batch Batch,
	_ CapturedGroup,
	settledPayload, payoutPayload []byte,
) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.simulateDuplicate {
		return true, nil
	}

	f.batches = append(f.batches, batch)
	f.outbox["payments.settled"] = append(f.outbox["payments.settled"], settledPayload)
	f.outbox["payouts.requested"] = append(f.outbox["payouts.requested"], payoutPayload)
	return false, nil
}

// ---- helpers ----------------------------------------------------------------

func testConfig() Config {
	return Config{
		WindowSize: 1 * time.Hour,
		Buffer:     5 * time.Minute,
		Interval:   1 * time.Minute,
	}
}

func testWindow() (start, end time.Time) {
	end = time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	start = end.Add(-1 * time.Hour)
	return
}

// ---- tests ------------------------------------------------------------------

// TestSettleHappyPath: two groups (different accounts, same currency) →
// two batches, two settled events, two payout events.
func TestSettleHappyPath(t *testing.T) {
	groups := []CapturedGroup{
		{AccountID: "acc_A", Currency: "INR", TotalMinor: 100_00, TxnCount: 3,
			PaymentIDs: []string{"p1", "p2", "p3"}},
		{AccountID: "acc_B", Currency: "INR", TotalMinor: 50_00, TxnCount: 1,
			PaymentIDs: []string{"p4"}},
	}
	store := newFakeStore(groups)
	engine := New(store, testConfig(), noopLogger()).(*pgEngine)

	windowStart, windowEnd := testWindow()
	if err := engine.settle(context.Background(), windowStart, windowEnd); err != nil {
		t.Fatalf("settle returned error: %v", err)
	}

	// Two batches, one per account.
	if got := len(store.batches); got != 2 {
		t.Fatalf("want 2 batches, got %d", got)
	}

	// Correct totals on each batch.
	batchByAccount := map[string]Batch{}
	for _, b := range store.batches {
		batchByAccount[b.AccountID] = b
	}

	assertBatch(t, batchByAccount["acc_A"], 100_00, 3, windowStart, windowEnd)
	assertBatch(t, batchByAccount["acc_B"], 50_00, 1, windowStart, windowEnd)

	// One settled + one payout event per batch = 2 each.
	if n := len(store.outbox["payments.settled"]); n != 2 {
		t.Errorf("want 2 payments.settled events, got %d", n)
	}
	if n := len(store.outbox["payouts.requested"]); n != 2 {
		t.Errorf("want 2 payouts.requested events, got %d", n)
	}
}

// TestSettleMultiCurrency: same account, two currencies → two batches.
// Money in different currencies must never be netted together.
func TestSettleMultiCurrency(t *testing.T) {
	groups := []CapturedGroup{
		{AccountID: "acc_A", Currency: "INR", TotalMinor: 500_00, TxnCount: 2,
			PaymentIDs: []string{"p1", "p2"}},
		{AccountID: "acc_A", Currency: "USD", TotalMinor: 100_00, TxnCount: 1,
			PaymentIDs: []string{"p3"}},
	}
	store := newFakeStore(groups)
	engine := New(store, testConfig(), noopLogger()).(*pgEngine)

	windowStart, windowEnd := testWindow()
	if err := engine.settle(context.Background(), windowStart, windowEnd); err != nil {
		t.Fatalf("settle returned error: %v", err)
	}

	if got := len(store.batches); got != 2 {
		t.Fatalf("want 2 batches (one per currency), got %d", got)
	}
}

// TestSettleEmptyWindow: no CAPTURED transactions → no batches, no events.
func TestSettleEmptyWindow(t *testing.T) {
	store := newFakeStore(nil)
	engine := New(store, testConfig(), noopLogger()).(*pgEngine)

	windowStart, windowEnd := testWindow()
	if err := engine.settle(context.Background(), windowStart, windowEnd); err != nil {
		t.Fatalf("settle returned error: %v", err)
	}

	if got := len(store.batches); got != 0 {
		t.Errorf("want 0 batches, got %d", got)
	}
}

// TestSettleIdempotent: running settle twice over the same window must produce
// exactly one batch (the second run detects alreadyExisted=true and skips).
// This is the chaos-test scenario: engine restarts mid-run.
func TestSettleIdempotent(t *testing.T) {
	groups := []CapturedGroup{
		{AccountID: "acc_A", Currency: "INR", TotalMinor: 200_00, TxnCount: 2,
			PaymentIDs: []string{"p1", "p2"}},
	}
	store := newFakeStore(groups)
	engine := New(store, testConfig(), noopLogger()).(*pgEngine)

	windowStart, windowEnd := testWindow()

	// First run — normal.
	if err := engine.settle(context.Background(), windowStart, windowEnd); err != nil {
		t.Fatalf("first settle: %v", err)
	}

	// Simulate what the DB unique constraint does on a second run.
	store.simulateDuplicate = true

	// Second run — must be a no-op.
	if err := engine.settle(context.Background(), windowStart, windowEnd); err != nil {
		t.Fatalf("second settle: %v", err)
	}

	// Still exactly one batch, one settled event, one payout.
	if got := len(store.batches); got != 1 {
		t.Errorf("want 1 batch after two runs, got %d", got)
	}
	if n := len(store.outbox["payments.settled"]); n != 1 {
		t.Errorf("want 1 payments.settled event, got %d", n)
	}
}

// TestSettledEventPayload: verify the JSON shape of the events that will land
// in Kafka so the ledger/analytics consumers can rely on the contract.
func TestSettledEventPayload(t *testing.T) {
	groups := []CapturedGroup{
		{AccountID: "acc_X", Currency: "INR", TotalMinor: 99_99, TxnCount: 7,
			PaymentIDs: []string{"p1"}},
	}
	store := newFakeStore(groups)
	engine := New(store, testConfig(), noopLogger()).(*pgEngine)

	windowStart, windowEnd := testWindow()
	_ = engine.settle(context.Background(), windowStart, windowEnd)

	raw := store.outbox["payments.settled"][0]
	var evt SettledEvent
	if err := json.Unmarshal(raw, &evt); err != nil {
		t.Fatalf("unmarshal SettledEvent: %v", err)
	}

	if evt.AccountID != "acc_X" {
		t.Errorf("account_id: want acc_X, got %s", evt.AccountID)
	}
	if evt.TotalMinor != 99_99 {
		t.Errorf("total_minor: want 9999, got %d", evt.TotalMinor)
	}
	if evt.TxnCount != 7 {
		t.Errorf("txn_count: want 7, got %d", evt.TxnCount)
	}
	if evt.BatchID == "" {
		t.Error("batch_id must not be empty")
	}

	raw = store.outbox["payouts.requested"][0]
	var payout PayoutRequestedEvent
	if err := json.Unmarshal(raw, &payout); err != nil {
		t.Fatalf("unmarshal PayoutRequestedEvent: %v", err)
	}
	if payout.BatchID != evt.BatchID {
		t.Errorf("payout batch_id must match settled batch_id")
	}
	if payout.TotalMinor != 99_99 {
		t.Errorf("payout total_minor: want 9999, got %d", payout.TotalMinor)
	}
}

// ---- assertion helpers ------------------------------------------------------

func assertBatch(t *testing.T, b Batch, wantMinor int64, wantCount int, wantStart, wantEnd time.Time) {
	t.Helper()
	if b.TotalMinor != wantMinor {
		t.Errorf("batch %s: total_minor want %d got %d", b.AccountID, wantMinor, b.TotalMinor)
	}
	if b.TxnCount != wantCount {
		t.Errorf("batch %s: txn_count want %d got %d", b.AccountID, wantCount, b.TxnCount)
	}
	if !b.WindowStart.Equal(wantStart) {
		t.Errorf("batch %s: window_start want %v got %v", b.AccountID, wantStart, b.WindowStart)
	}
	if !b.WindowEnd.Equal(wantEnd) {
		t.Errorf("batch %s: window_end want %v got %v", b.AccountID, wantEnd, b.WindowEnd)
	}
	if b.ID == "" {
		t.Errorf("batch %s: ID must not be empty", b.AccountID)
	}
	if b.Status != BatchStatusPending {
		t.Errorf("batch %s: status want PENDING got %s", b.AccountID, b.Status)
	}
}

func noopLogger() *slog.Logger {
	return slog.Default()
}
