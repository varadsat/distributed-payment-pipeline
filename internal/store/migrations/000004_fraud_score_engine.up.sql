CREATE TABLE fraud_scores (
    id          BIGSERIAL PRIMARY KEY,
    payment_id  UUID        NOT NULL,
    account_id  TEXT        NOT NULL,
    score       NUMERIC     NOT NULL,       -- 0.0 to 1.0
    risk_level  TEXT        NOT NULL,
    signals     JSONB       NOT NULL,       -- array of reason strings
    reviewed_by TEXT,                       -- null until a human reviews
    reviewed_at TIMESTAMPTZ,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_fraud_payment   ON fraud_scores (payment_id);
CREATE INDEX idx_fraud_risk      ON fraud_scores (risk_level, created_at);
CREATE INDEX idx_fraud_unreviewed ON fraud_scores (created_at)
    WHERE reviewed_at IS NULL AND risk_level IN ('HIGH', 'CRITICAL');