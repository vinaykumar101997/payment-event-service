-- Set once by HeldPaymentSweeper when a payment has been HELD longer than
-- payments.screening.timeout. Only a marker for people and alerts: nothing reads it to
-- release or reject a payment. NULL for every payment that was never overdue.
ALTER TABLE payments ADD COLUMN overdue_at TIMESTAMPTZ;
