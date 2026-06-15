// Package idempotency provides the dedup hot-path check backed by Redis.
package idempotency

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

// Store guards against double-processing of retried submissions.
type Store interface {
	// Claim atomically reserves the key (SET key val NX EX ttl). Returns
	// (claimed=true) if this caller won the slot, or the cached payment_id of
	// the prior submission if it was already processed.
	Claim(ctx context.Context, key, paymentID string) (claimed bool, existingPaymentID string, err error)
}

// DeriveKey returns the client-supplied idempotency key if present, otherwise a
// deterministic hash of the identifying fields (fallback dedup).
func DeriveKey(clientKey, source, externalTxnID, accountID string, amountMinor int64, currency string) string {
	if clientKey != "" {
		return clientKey
	}
	h := sha256.Sum256([]byte(fmt.Sprintf("%s|%s|%s|%d|%s",
		source, externalTxnID, accountID, amountMinor, currency)))
	return "derived:" + hex.EncodeToString(h[:])
}

// TODO: redisStore implements Store using go-redis with `SET key val NX EX ttl`.
