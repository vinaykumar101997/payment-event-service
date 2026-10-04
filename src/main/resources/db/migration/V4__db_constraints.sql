-- Defence in depth: PaymentRequest/PaymentController/PaymentService already enforce these
-- rules, but only on the paths that go through them. The database enforces them for every
-- writer (a bug in a new code path, a script, a manual fix). Named explicitly so a
-- violation's error message says which rule was broken (SQLState 23514 check_violation).
ALTER TABLE accounts
    ADD CONSTRAINT accounts_balance_non_negative CHECK (balance >= 0);

ALTER TABLE payments
    ADD CONSTRAINT payments_amount_positive CHECK (amount > 0);

ALTER TABLE payments
    ADD CONSTRAINT payments_distinct_accounts CHECK (from_account <> to_account);
