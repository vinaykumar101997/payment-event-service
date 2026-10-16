-- One row per screening verdict event applied (or deliberately not applied) to a payment.
-- The primary key (payment_id, event_id) is the idempotency key: a redelivered event hits
-- ON CONFLICT DO NOTHING and changes nothing. outcome records what the event did, so a
-- verdict that arrived after the payment had already left HELD is visible as ignored.
CREATE TABLE screening_verdicts (
    payment_id        VARCHAR(64)       NOT NULL REFERENCES payments (payment_id),
    event_id          UUID              NOT NULL,
    verdict           VARCHAR(10)       NOT NULL,
    reasons           JSONB             NOT NULL,
    risk_score        DOUBLE PRECISION,
    decided_at        TIMESTAMPTZ       NOT NULL,
    screening_run_id  VARCHAR(128)      NOT NULL,
    outcome           VARCHAR(20)       NOT NULL,
    received_at       TIMESTAMPTZ       NOT NULL DEFAULT now(),
    PRIMARY KEY (payment_id, event_id),
    CONSTRAINT screening_verdicts_verdict_known CHECK (verdict IN ('CLEAR', 'FLAG', 'REJECT', 'ERROR')),
    CONSTRAINT screening_verdicts_outcome_known
        CHECK (outcome IN ('RELEASED', 'REJECTED', 'KEPT_HELD', 'IGNORED_NOT_HELD'))
);
