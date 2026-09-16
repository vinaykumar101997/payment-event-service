CREATE TABLE accounts (
    id          VARCHAR(64)     PRIMARY KEY,
    balance     NUMERIC(19, 2)  NOT NULL,
    currency    VARCHAR(3)      NOT NULL,
    created_at  TIMESTAMPTZ     NOT NULL DEFAULT now()
);

CREATE TABLE payments (
    payment_id      VARCHAR(64)     PRIMARY KEY,
    from_account    VARCHAR(64)     NOT NULL REFERENCES accounts (id),
    to_account      VARCHAR(64)     NOT NULL REFERENCES accounts (id),
    amount          NUMERIC(19, 2)  NOT NULL,
    currency        VARCHAR(3)      NOT NULL,
    status          VARCHAR(20)     NOT NULL,
    failure_reason  VARCHAR(500),
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT now(),
    processed_at    TIMESTAMPTZ,
    last_swept_at   TIMESTAMPTZ
);

CREATE INDEX idx_payments_status ON payments (status);
CREATE INDEX idx_payments_status_created_at ON payments (status, created_at);
