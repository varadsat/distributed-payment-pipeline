package intake

import (
	"context"
	"errors"
	"testing"

	paymentv1 "github.com/varadsat/distributed-payment-pipeline/gen/payment/v1"
	"github.com/varadsat/distributed-payment-pipeline/internal/domain"
	"github.com/varadsat/distributed-payment-pipeline/internal/normalize"
)

type fakeStore struct {
	saveErr          error
	byPaymentID      map[string]domain.Transaction
	byIdempotencyKey map[string]domain.Transaction
	saved            []domain.Transaction
}

func (f *fakeStore) SaveWithOutbox(ctx context.Context, t domain.Transaction, outboxPayload []byte) error {
	return nil
}

func (f *fakeStore) UpdateState(ctx context.Context, paymentID string, from, to domain.State) error {
	return nil
}

func (f *fakeStore) GetByPaymentID(ctx context.Context, paymentID string) (domain.Transaction, error) {
	if tx, ok := f.byPaymentID[paymentID]; ok {
		return tx, nil
	}
	return domain.Transaction{}, errors.New("transaction not found")
}

func (f *fakeStore) GetByIdempotencyKey(ctx context.Context, key string) (domain.Transaction, error) {
	if tx, ok := f.byIdempotencyKey[key]; ok {
		return tx, nil
	}
	return domain.Transaction{}, errors.New("transaction not found")
}

func (f *fakeStore) Save(ctx context.Context, t domain.Transaction) error {
	if f.saveErr != nil {
		return f.saveErr
	}
	f.saved = append(f.saved, t)
	return nil
}

func (f *fakeStore) Close() {}

func TestSubmitPaymentSavesTransactionAndReturnsResponse(t *testing.T) {
	ctx := context.Background()
	store := &fakeStore{}
	registry := normalize.NewRegistry()
	registry.Register("CARD", 1, &normalize.CardNormalizer{})
	server := &Server{
		Store:       store,
		Normalizers: registry,
	}

	req := &paymentv1.SubmitPaymentRequest{
		IdempotencyKey: "idem-123",
		Source:         paymentv1.PaymentSource_PAYMENT_SOURCE_CARD,
		ExternalTxnId:  "ext-123",
		AccountId:      "acct-123",
		Amount: &paymentv1.Money{
			MinorUnits: 2500,
			Currency:   "USD",
		},
		SchemaVersion: 1,
		Metadata: map[string]string{
			"channel": "web",
		},
	}

	resp, err := server.SubmitPayment(ctx, req)
	if err != nil {
		t.Fatalf("SubmitPayment() error = %v", err)
	}

	if resp == nil {
		t.Fatal("SubmitPayment() returned nil response")
	}

	if len(store.saved) != 1 {
		t.Fatalf("Save() calls = %d, want 1", len(store.saved))
	}

	saved := store.saved[0]
	if saved.IdempotencyKey != req.IdempotencyKey {
		t.Errorf("saved idempotency key = %q, want %q", saved.IdempotencyKey, req.IdempotencyKey)
	}
	if saved.Source != domain.SourceCard {
		t.Errorf("saved source = %q, want %q", saved.Source, domain.SourceCard)
	}
	if saved.Amount.MinorUnits != req.Amount.MinorUnits || saved.Amount.Currency != req.Amount.Currency {
		t.Errorf("saved amount = %+v, want %+v", saved.Amount, req.Amount)
	}
	if saved.State != domain.StateReceived {
		t.Errorf("saved state = %q, want %q", saved.State, domain.StateReceived)
	}
	if resp.PaymentId != saved.PaymentID {
		t.Errorf("response payment ID = %q, want %q", resp.PaymentId, saved.PaymentID)
	}
	if resp.State != paymentv1.PaymentState_PAYMENT_STATE_RECEIVED {
		t.Errorf("response state = %v, want %v", resp.State, paymentv1.PaymentState_PAYMENT_STATE_RECEIVED)
	}
	if resp.ReceivedAt == nil || !resp.ReceivedAt.AsTime().Equal(saved.CreatedAt) {
		t.Errorf("received timestamp = %v, want %v", resp.ReceivedAt, saved.CreatedAt)
	}
}

func TestSubmitPaymentReturnsStoreError(t *testing.T) {
	ctx := context.Background()
	expectedErr := errors.New("save failed")
	registry := normalize.NewRegistry()
	registry.Register("CARD", 1, &normalize.CardNormalizer{})
	server := &Server{
		Store:       &fakeStore{saveErr: expectedErr},
		Normalizers: registry,
	}

	resp, err := server.SubmitPayment(ctx, &paymentv1.SubmitPaymentRequest{
		IdempotencyKey: "idem-456",
		Source:         paymentv1.PaymentSource_PAYMENT_SOURCE_CARD,
		Amount: &paymentv1.Money{
			MinorUnits: 100,
			Currency:   "USD",
		},
		SchemaVersion: 1,
	})
	if resp != nil {
		t.Errorf("SubmitPayment() response = %+v, want nil", resp)
	}
	if !errors.Is(err, expectedErr) {
		t.Fatalf("SubmitPayment() error = %v, want %v", err, expectedErr)
	}
}
