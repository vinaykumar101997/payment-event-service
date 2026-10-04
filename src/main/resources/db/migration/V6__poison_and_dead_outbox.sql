-- Poison payments: processing attempts that failed with a non-business error. Incremented
-- in its own transaction (the failed attempt itself rolls back), and once it reaches the
-- configured maximum the payment moves to the terminal POISONED status.
ALTER TABLE payments ADD COLUMN processing_attempts INTEGER NOT NULL DEFAULT 0;

-- Dead outbox rows: publishing failed the configured maximum number of times. They stay in
-- the table (published_at NULL, dead_at set) for investigation and are never claimed again.
ALTER TABLE outbox_events ADD COLUMN dead_at TIMESTAMPTZ;

DROP INDEX idx_outbox_events_pending;
CREATE INDEX idx_outbox_events_pending ON outbox_events (id) WHERE published_at IS NULL AND dead_at IS NULL;
CREATE INDEX idx_outbox_events_dead ON outbox_events (dead_at) WHERE dead_at IS NOT NULL;
