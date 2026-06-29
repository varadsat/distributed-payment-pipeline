-- Enforce exactly one settlement batch per accountid and currency within a window.
-- conflicts and is silently skipped via ON CONFLICT DO NOTHING in the INSERT.
CREATE UNIQUE INDEX idx_settlement_window
    ON settlement_batches (account_id, currency, window_start, window_end);
