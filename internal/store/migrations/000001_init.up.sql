-- Canonical transaction records.
CREATE TABLE transactions (
    payment_id      UUID PRIMARY KEY,
    idempotency_key TEXT        NOT NULL,
    source          TEXT        NOT NULL,
    external_txn_id TEXT,
    account_id      TEXT        NOT NULL,
    amount_minor    BIGINT      NOT NULL,
    currency        CHAR(3)     NOT NULL,
    state           TEXT        NOT NULL,
    schema_version  INT         NOT NULL DEFAULT 1,
    metadata        JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX idx_txn_idempotency    ON transactions (idempotency_key);
CREATE INDEX        idx_txn_account_created ON transactions (account_id, created_at);

-- Transactional outbox: written in the SAME tx as the transaction row.
CREATE TABLE outbox (
    id            BIGSERIAL PRIMARY KEY,
    aggregate_id  UUID        NOT NULL,
    topic         TEXT        NOT NULL,
    partition_key TEXT        NOT NULL, -- account_id, for ordering
    payload       JSONB       NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at  TIMESTAMPTZ
);
-- Partial index so the relay only scans unpublished rows.
CREATE INDEX idx_outbox_unpublished ON outbox (id) WHERE published_at IS NULL;

-- Double-entry ledger. Per payment, debit + credit legs must sum to zero.
CREATE TABLE ledger_entries (
    id           BIGSERIAL PRIMARY KEY,
    payment_id   UUID        NOT NULL,
    account      TEXT        NOT NULL,
    direction    TEXT        NOT NULL CHECK (direction IN ('DEBIT','CREDIT')),
    amount_minor BIGINT      NOT NULL,
    currency     CHAR(3)     NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_ledger_payment ON ledger_entries (payment_id);

-- Audit log of every state transition.
CREATE TABLE state_transitions (
    id         BIGSERIAL PRIMARY KEY,
    payment_id UUID        NOT NULL,
    from_state TEXT,
    to_state   TEXT        NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Settlement batches produced by the settlement engine.
CREATE TABLE settlement_batches (
    id           UUID PRIMARY KEY,
    account_id   TEXT        NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end   TIMESTAMPTZ NOT NULL,
    total_minor  BIGINT      NOT NULL,
    currency     CHAR(3)     NOT NULL,
    txn_count    INT         NOT NULL,
    status       TEXT        NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
