-- A compliance officer's release or reject of a HELD payment (POST /payments/{id}/release,
-- /reject). One row per payment at most: the primary key is what makes a repeated request
-- idempotent and a contradicting one impossible. Never updated or deleted.
CREATE TABLE officer_decisions (
    payment_id  VARCHAR(64)    PRIMARY KEY REFERENCES payments (payment_id),
    decision    VARCHAR(10)    NOT NULL,
    officer     VARCHAR(100)   NOT NULL,
    note        VARCHAR(1000)  NOT NULL,
    decided_at  TIMESTAMPTZ    NOT NULL DEFAULT now(),
    CONSTRAINT officer_decisions_decision_known CHECK (decision IN ('RELEASE', 'REJECT'))
);
