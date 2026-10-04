-- Transactional outbox: events are inserted here in the same transaction as the state
-- change they describe, then published to Kafka by OutboxRelay. A row with published_at
-- NULL is pending; next_attempt_at backs off rows whose publish failed, so one bad row
-- can't block the rows behind it.
CREATE TABLE outbox_events (
    id               BIGSERIAL       PRIMARY KEY,
    topic            VARCHAR(255)    NOT NULL,
    message_key      VARCHAR(64)     NOT NULL,
    payload          TEXT            NOT NULL,
    created_at       TIMESTAMPTZ     NOT NULL DEFAULT now(),
    published_at     TIMESTAMPTZ,
    attempts         INTEGER         NOT NULL DEFAULT 0,
    next_attempt_at  TIMESTAMPTZ     NOT NULL DEFAULT now(),
    last_error       VARCHAR(1000)
);

CREATE INDEX idx_outbox_events_pending ON outbox_events (id) WHERE published_at IS NULL;
CREATE INDEX idx_outbox_events_message_key ON outbox_events (message_key);
