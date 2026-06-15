// Package intake implements the gRPC PaymentIntake service: the inbound layer
// that ties together normalize -> validate -> idempotency -> store(+outbox).
package intake

import (
	"context"

	"github.com/yourname/payment-pipeline/internal/idempotency"
	"github.com/yourname/payment-pipeline/internal/normalize"
	"github.com/yourname/payment-pipeline/internal/store"
	"github.com/yourname/payment-pipeline/internal/validate"
)

// Server wires the intake pipeline together. It implements the generated
// paymentv1.PaymentIntakeServer interface (embed UnimplementedServer once
// `make proto` has generated the code).
type Server struct {
	Normalizers *normalize.Registry
	Validator   validate.Validator
	Idem        idempotency.Store
	Store       store.Store
	// paymentv1.UnimplementedPaymentIntakeServer
}

// SubmitPayment flow:
//   1. normalize raw payload -> domain.Transaction
//   2. derive idempotency key, Claim() in Redis; if already seen, return cached ack
//   3. validate
//   4. SaveWithOutbox (transaction row + outbox row in one DB tx)
//   5. return ack (the relay publishes to Kafka asynchronously)
func (s *Server) SubmitPayment(ctx context.Context /*, req *paymentv1.SubmitPaymentRequest */) error {
	// TODO: implement the five steps above.
	return nil
}

// BatchSubmitPayment consumes a client stream, applying SubmitPayment per item.
// TODO: implement streaming receive loop + aggregate response counts.
