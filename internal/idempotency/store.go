// Package idempotency provides the dedup hot-path check backed by Redis.
package idempotency

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/varadsat/distributed-payment-pipeline/internal/domain"
)

// Store guards against double-processing of retried submissions.
type Store interface {
	// Claim atomically reserves the key (SET key val NX EX ttl). Returns
	// (claimed=true) if this caller won the slot, or the cached payment_id of
	// the prior submission if it was already processed.
	Claim(ctx context.Context, key, paymentID string) (claimed bool, existingPaymentID string, err error)
}

// DeriveKey returns the transaction's client-supplied idempotency key if
// present, otherwise a deterministic hash of the identifying fields.
func DeriveKey(tx domain.Transaction) string {
	if tx.IdempotencyKey != "" {
		return tx.IdempotencyKey
	}
	h := sha256.Sum256([]byte(fmt.Sprintf("%s|%s|%s|%d|%s",
		tx.Source,
		tx.ExternalTxnID,
		tx.AccountID,
		tx.Amount.MinorUnits,
		tx.Amount.Currency,
	)))
	return "derived:" + hex.EncodeToString(h[:])
}

// TODO: redisStore implements Store using go-redis with `SET key val NX EX ttl`.

type redisStore struct {
	// Redis client and config (e.g. TTL) would go here.
	redisClient *redis.Client
	ttlSeconds  int
}

func NewRedisStore(redisClient *redis.Client, ttlSeconds int) Store {
	return &redisStore{
		redisClient: redisClient,
		ttlSeconds:  ttlSeconds,
	}
}

func (s *redisStore) Claim(ctx context.Context, key, paymentID string) (bool, string, error) {
	// Implement the Redis SET NX EX logic here, returning (claimed, existingPaymentID, err).
	// If SET returns that the key already exists, fetch the existing payment_id to return.
	setResult, err := s.redisClient.SetNX(ctx, key, paymentID, time.Duration(s.ttlSeconds)*time.Second).Result()
	if err != nil {
		return false, "", err
	}
	if setResult {
		// Successfully claimed the key
		return true, "", nil
	}
	// Key already exists, fetch the existing payment_id
	existingPaymentID, err := s.redisClient.Get(ctx, key).Result()
	if err != nil {
		return false, "", err
	}
	return false, existingPaymentID, nil
}
