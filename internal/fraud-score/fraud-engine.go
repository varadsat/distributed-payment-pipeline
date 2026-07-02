package fraudscore

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/varadsat/distributed-payment-pipeline/internal/domain"
)

type Score struct {
	PaymentID string
	AccountID string
	RiskLevel RiskLevel
	Signals   []string
	Score     float64
}

type RiskLevel string

const (
	RiskLow      RiskLevel = "LOW"
	RiskMedium   RiskLevel = "MEDIUM"
	RiskHigh     RiskLevel = "HIGH"
	RiskCritical RiskLevel = "CRITICAL"
)

type FraudEngine struct {
	pool        *pgxpool.Pool
	redisClient *redis.Client
	logger      *slog.Logger
}

func NewFraudEngine(pool *pgxpool.Pool, redisClient *redis.Client, logger *slog.Logger) *FraudEngine {
	return &FraudEngine{
		pool:        pool,
		redisClient: redisClient,
		logger:      logger,
	}
}

func (e *FraudEngine) Score(ctx context.Context, t domain.Transaction) Score {
	var signals []string
	var score float64

	// Rule 1: amount threshold — flag anything over ₹10,000
	if t.Amount.MinorUnits > 1_000_000 {
		signals = append(signals, "amount exceeds threshold")
		score += 0.4
	}

	// Rule 2: velocity — more than 10 txns/min from the same account.
	// INCR returns the value after increment; we set TTL only on the first
	// increment (when the key didn't exist before) using Expire separately.
	// Using a pipeline so both commands go in one round-trip.
	velocityKey := "velocity:" + t.AccountID
	pipe := e.redisClient.Pipeline()
	incrCmd := pipe.Incr(ctx, velocityKey)
	pipe.Expire(ctx, velocityKey, 1*time.Minute)
	if _, err := pipe.Exec(ctx); err != nil {
		// Non-fatal: log and skip the velocity rule rather than failing the
		// whole score. Fraud scoring should never block the payment pipeline.
		e.logger.Warn("fraud velocity check failed", "account_id", t.AccountID, "err", err)
	} else if incrCmd.Val() > 10 {
		signals = append(signals, "velocity: >10 txns/min for account")
		score += 0.5
	}

	// Rule 3: new account + large amount
	if t.Metadata["account_age_days"] == "0" && t.Amount.MinorUnits > 500_000 {
		signals = append(signals, "new account with large transaction")
		score += 0.3
	}

	finalScore := min(score, 1.0)

	return Score{
		PaymentID: t.PaymentID,
		AccountID: t.AccountID,
		Score:     finalScore,
		RiskLevel: toRiskLevel(finalScore),
		Signals:   signals,
	}
}

func toRiskLevel(score float64) RiskLevel {
	switch {
	case score >= 0.8:
		return RiskCritical
	case score >= 0.5:
		return RiskHigh
	case score >= 0.2:
		return RiskMedium
	default:
		return RiskLow
	}
}

func (e *FraudEngine) SaveScore(ctx context.Context, s Score) error {
	// Encode the signals slice as JSON for the JSONB column.
	signalsJSON, err := json.Marshal(s.Signals)
	if err != nil {
		return fmt.Errorf("SaveScore: marshal signals: %w", err)
	}

	_, err = e.pool.Exec(ctx, `
		INSERT INTO fraud_scores (
			payment_id,
			account_id,
			score,
			risk_level,
			signals,
			created_at
		) VALUES ($1, $2, $3, $4, $5, now())
		ON CONFLICT DO NOTHING
	`, s.PaymentID, s.AccountID, s.Score, string(s.RiskLevel), signalsJSON)
	if err != nil {
		return fmt.Errorf("SaveScore: insert: %w", err)
	}

	return nil
}
