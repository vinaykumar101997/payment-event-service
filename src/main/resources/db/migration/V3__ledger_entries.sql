-- Double-entry ledger: every PROCESSED payment writes one debit (negative amount, from
-- account) and one credit (positive amount, to account), in the same transaction as the
-- balance change. ReconciliationService checks the two invariants this enables.
CREATE TABLE ledger_entries (
    id          BIGSERIAL       PRIMARY KEY,
    payment_id  VARCHAR(64)     NOT NULL REFERENCES payments (payment_id),
    account_id  VARCHAR(64)     NOT NULL REFERENCES accounts (id),
    amount      NUMERIC(19, 2)  NOT NULL,
    currency    VARCHAR(3)      NOT NULL,
    created_at  TIMESTAMPTZ     NOT NULL DEFAULT now()
);

CREATE INDEX idx_ledger_entries_payment_id ON ledger_entries (payment_id);
CREATE INDEX idx_ledger_entries_account_id ON ledger_entries (account_id);

-- Reconciliation needs a starting point: balance = opening_balance + sum(entries).
-- Existing rows have no ledger history, so their current balance becomes the opening
-- balance - the ledger is authoritative from this migration onward, not retroactively.
ALTER TABLE accounts ADD COLUMN opening_balance NUMERIC(19, 2);
UPDATE accounts SET opening_balance = balance;
ALTER TABLE accounts ALTER COLUMN opening_balance SET NOT NULL;
