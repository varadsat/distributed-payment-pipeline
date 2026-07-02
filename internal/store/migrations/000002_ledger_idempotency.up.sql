-- Enforce exactly one DEBIT and one CREDIT per payment_id.
-- This makes DoubleEntryPoster.Post idempotent: a duplicate Kafka delivery
-- (relay at-least-once) or a consumer crash before offset commit simply
-- conflicts and is silently skipped via ON CONFLICT DO NOTHING in the INSERT.
CREATE UNIQUE INDEX idx_ledger_payment_direction
    ON ledger_entries (payment_id, direction);
