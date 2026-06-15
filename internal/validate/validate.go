// Package validate runs field-level and business-rule checks on a Transaction.
package validate

import "github.com/yourname/payment-pipeline/internal/domain"

// Validator enforces field-level checks (currency code, positive amount) and
// business rules (account exists/active, amount within limits). Business-rule
// lookups should hit Redis-cached account/config data to stay on the hot path.
type Validator interface {
	Validate(t domain.Transaction) error
}

// TODO: implement field checks + a cache-backed business-rule checker.
