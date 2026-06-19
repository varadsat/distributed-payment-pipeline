// Package intake implements the gRPC PaymentIntake service: the inbound layer
// that ties together normalize -> validate -> idempotency -> store(+outbox).
package intake

import (
	"context"
	"time"

	"github.com/google/uuid"
	paymentv1 "github.com/varadsat/distributed-payment-pipeline/gen/payment/v1"
	"github.com/varadsat/distributed-payment-pipeline/internal/domain"
	"github.com/varadsat/distributed-payment-pipeline/internal/idempotency"
	"github.com/varadsat/distributed-payment-pipeline/internal/normalize"
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
	paymentv1.UnimplementedPaymentIntakeServer
}

// SubmitPayment flow:
//  1. normalize raw payload -> domain.Transaction
//  2. derive idempotency key, Claim() in Redis; if already seen, return cached ack
//  3. validate
//  4. SaveWithOutbox (transaction row + outbox row in one DB tx)
//  5. return ack (the relay publishes to Kafka asynchronously)
func (s *Server) SubmitPayment(ctx context.Context, req *paymentv1.SubmitPaymentRequest) (*paymentv1.SubmitPaymentResponse, error) {
	var amount domain.Money
	if req.GetAmount() != nil {
		amount = domain.Money{
			MinorUnits: req.GetAmount().MinorUnits,
			Currency:   req.GetAmount().Currency,
		}
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
		source = ""
	}

	now := time.Now()
	transaction := domain.Transaction{
		PaymentID:      uuid.NewString(),
		IdempotencyKey: req.IdempotencyKey,
		Source:         source,
		ExternalTxnID:  req.ExternalTxnId,
		AccountID:      req.AccountId,
		Amount:         amount,
		State:          domain.StateReceived,
		SchemaVersion:  req.SchemaVersion,
		Metadata:       req.Metadata,
		CreatedAt:      now,
		UpdatedAt:      now,
	}

	if err := s.Store.Save(ctx, transaction); err != nil {
		return nil, err
	}

	return &paymentv1.SubmitPaymentResponse{
		PaymentId:    transaction.PaymentID,
		State:        paymentv1.PaymentState_PAYMENT_STATE_RECEIVED,
		ReceivedAt:   timestamppb.New(transaction.CreatedAt),
		Deduplicated: false,
	}, nil
}
