// Package validate runs field-level and business-rule checks on a Transaction.
package validate

import (
	"context"
	"fmt"
	"strings"

	"github.com/varadsat/distributed-payment-pipeline/internal/domain"
)

// Validator enforces field-level checks (currency code, positive amount) and
// business rules (account exists/active, amount within limits). Business-rule
// lookups should hit Redis-cached account/config data to stay on the hot path.
type Validator interface {
	Validate(t domain.Transaction) error
}

// RedisClient is a minimal contract for the account-status cache.
// We store the account activity state under a Redis key such as
// "account:active:<account_id>" and keep the value as one of:
// "active", "inactive", "1", or "0".
type RedisClient interface {
	Get(ctx context.Context, key string) (string, error)
}

// DefaultValidator performs the field-level checks that previously lived in
// the normalizers.
type DefaultValidator struct{}

func (v *DefaultValidator) Validate(t domain.Transaction) error {
	if strings.TrimSpace(t.Amount.Currency) == "" {
		return fmt.Errorf("currency is required")
	}
	if t.Amount.MinorUnits <= 0 {
		return fmt.Errorf("amount must be positive")
	}

	switch t.Source {
	case domain.SourceCard:
		if t.Metadata != nil && strings.TrimSpace(t.Metadata["card_number"]) == "" {
			return fmt.Errorf("card_number is required")
		}
	case domain.SourceUPI:
		if t.Metadata != nil && strings.TrimSpace(t.Metadata["upi_id"]) == "" {
			return fmt.Errorf("upi_id is required")
		}
	}

	return nil
}

type AccountValidator struct {
	// Redis client to check account existence/active status and limits.
	redisClient RedisClient
}

func (v *AccountValidator) Validate(t domain.Transaction) error {
	if strings.TrimSpace(t.AccountID) == "" {
		return fmt.Errorf("account_id is required")
	}
	if v.redisClient == nil {
		return fmt.Errorf("redis client is not configured")
	}

	key := accountActiveKey(t.AccountID)
	value, err := v.redisClient.Get(context.Background(), key)
	if err != nil {
		return fmt.Errorf("lookup account status for %s: %w", t.AccountID, err)
	}

	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "true", "active", "enabled":
		return nil
	case "0", "false", "inactive", "disabled":
		return fmt.Errorf("account %s is inactive", t.AccountID)
	default:
		return fmt.Errorf("invalid account status %q for %s", value, t.AccountID)
	}
}

func accountActiveKey(accountID string) string {
	return "account:active:" + accountID
}
