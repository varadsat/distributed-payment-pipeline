// Package intake implements the gRPC PaymentIntake service: the inbound layer
// that ties together normalize -> validate -> idempotency -> store(+outbox).
package intake

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	paymentv1 "github.com/varadsat/distributed-payment-pipeline/gen/payment/v1"
	"github.com/varadsat/distributed-payment-pipeline/internal/domain"
	"github.com/varadsat/distributed-payment-pipeline/internal/idempotency"
	"github.com/varadsat/distributed-payment-pipeline/internal/normalize"
	"github.com/varadsat/distributed-payment-pipeline/internal/outbox"
	"github.com/varadsat/distributed-payment-pipeline/internal/store"
	"github.com/varadsat/distributed-payment-pipeline/internal/validate"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Server wires the intake pipeline together. It implements the generated
// paymentv1.PaymentIntakeServer interface (embed UnimplementedServer once
// `make proto` has generated the code).
type Server struct {
	Normalizers *normalize.Registry
	Validator   validate.Validator
	Idem        idempotency.Store
	Store       store.Store
	Logger      *slog.Logger
	paymentv1.UnimplementedPaymentIntakeServer
}

// SubmitPayment flow:
//  1. normalize raw payload -> domain.Transaction
//  2. derive idempotency key, Claim() in Redis; if already seen, return cached ack
//  3. validate
//  4. SaveWithOutbox (transaction row + outbox row in one DB tx)
//  5. return ack (the relay publishes to Kafka asynchronously)
func (s *Server) SubmitPayment(ctx context.Context, req *paymentv1.SubmitPaymentRequest) (*paymentv1.SubmitPaymentResponse, error) {
	if s.Normalizers == nil {
		return nil, fmt.Errorf("normalizers not configured")
	}
	if s.Store == nil {
		return nil, fmt.Errorf("store not configured")
	}
	if s.Idem == nil {
		return nil, fmt.Errorf("idempotency store not configured")
	}

	logger := s.Logger
	if logger == nil {
		logger = slog.Default()
	}

	raw := map[string]string{
		"payment_id":      uuid.NewString(),
		"idempotency_key": req.GetIdempotencyKey(),
		"account_id":      req.GetAccountId(),
		"external_txn_id": req.GetExternalTxnId(),
		"currency":        req.GetAmount().GetCurrency(),
		"state":           string(domain.StateReceived),
	}
	if req.GetAmount() != nil {
		raw["amount"] = formatAmount(req.GetAmount().GetMinorUnits())
	}
	for key, value := range req.GetMetadata() {
		raw[key] = value
	}

	var source domain.Source
	switch req.GetSource() {
	case paymentv1.PaymentSource_PAYMENT_SOURCE_CARD:
		source = domain.SourceCard
	case paymentv1.PaymentSource_PAYMENT_SOURCE_UPI:
		source = domain.SourceUPI
	case paymentv1.PaymentSource_PAYMENT_SOURCE_BANK_TRANSFER:
		source = domain.SourceBankTransfer
	case paymentv1.PaymentSource_PAYMENT_SOURCE_WALLET:
		source = domain.SourceWallet
	default:
		logger.Error("unsupported payment source", "source", req.GetSource())
		return nil, fmt.Errorf("unsupported source: %v", req.GetSource())
	}

	logger.Debug("normalizing payment", "payment_id", raw["payment_id"], "source", source, "schema_version", req.GetSchemaVersion())

	normalizer, err := s.Normalizers.Get(string(source), int32(req.GetSchemaVersion()))
	if err != nil {
		logger.Error("normalizer not found", "source", source, "schema_version", req.GetSchemaVersion(), "error", err)
		return nil, err
	}

	transaction, err := normalizer.Normalize(raw)
	if err != nil {
		logger.Error("normalization failed", "payment_id", raw["payment_id"], "error", err)
		return nil, err
	}

	logger.Debug("payment normalized successfully", "payment_id", transaction.PaymentID)

	idempotencyKey := idempotency.DeriveKey(transaction)
	logger.Debug("claiming idempotency", "payment_id", transaction.PaymentID, "idempotency_key", idempotencyKey)

	claimed, existingPaymentID, err := s.Idem.Claim(ctx, idempotencyKey, transaction.PaymentID)
	if err != nil {
		logger.Error("idempotency claim failed", "payment_id", transaction.PaymentID, "error", err)
		return nil, fmt.Errorf("idempotency claim: %w", err)
	}
	if !claimed {
		// Idempotency key already exists, return cached ack with existing payment ID.
		logger.Warn("duplicate payment detected", "payment_id", transaction.PaymentID, "existing_payment_id", existingPaymentID)
		return &paymentv1.SubmitPaymentResponse{
			PaymentId:    existingPaymentID,
			State:        paymentv1.PaymentState_PAYMENT_STATE_RECEIVED,
			ReceivedAt:   timestamppb.Now(),
			Deduplicated: true,
		}, nil
	}

	if s.Validator != nil {
		logger.Debug("validating payment", "payment_id", transaction.PaymentID)
		if err := s.Validator.Validate(transaction); err != nil {
			logger.Error("validation failed", "payment_id", transaction.PaymentID, "error", err)
			return nil, err
		}
	}

	logger.Debug("transitioning state to VALIDATED", "payment_id", transaction.PaymentID)
	transaction.State = domain.StateValidated

	now := time.Now()
	transaction.CreatedAt = now
	transaction.UpdatedAt = now

	logger.Debug("marshaling outbox payload", "payment_id", transaction.PaymentID)
	outboxPayload, err := json.Marshal(outbox.NewPaymentReceivedEvent(transaction))
	if err != nil {
		logger.Error("failed to marshal outbox payload", "payment_id", transaction.PaymentID, "error", err)
		return nil, fmt.Errorf("marshal outbox payload: %w", err)
	}

	logger.Debug("saving transaction with outbox", "payment_id", transaction.PaymentID)
	if err := s.Store.SaveWithOutbox(ctx, transaction, outboxPayload); err != nil {
		logger.Error("failed to save transaction with outbox", "payment_id", transaction.PaymentID, "error", err)
		return nil, err
	}

	logger.Info("payment submitted successfully", "payment_id", transaction.PaymentID, "account_id", transaction.AccountID, "amount_minor", transaction.Amount.MinorUnits, "currency", transaction.Amount.Currency)
	return &paymentv1.SubmitPaymentResponse{
		PaymentId:    transaction.PaymentID,
		State:        paymentv1.PaymentState_PAYMENT_STATE_RECEIVED,
		ReceivedAt:   timestamppb.New(transaction.CreatedAt),
		Deduplicated: false,
	}, nil
}

func formatAmount(minorUnits int64) string {
	sign := ""
	value := minorUnits
	if value < 0 {
		sign = "-"
		value = -value
	}

	major := value / 100
	minor := value % 100
	return fmt.Sprintf("%s%d.%02d", sign, major, minor)
}
