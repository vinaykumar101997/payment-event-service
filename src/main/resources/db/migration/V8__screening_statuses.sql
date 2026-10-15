-- Screening hold (Stage 2) adds two statuses: HELD (waiting for a screening verdict or an
-- officer) and REJECTED (terminal, rejected by screening or an officer). status had no
-- constraint until now; listing every known value here means a typo or a status from a
-- newer build can't be written by any path. A new status needs a migration that replaces
-- this constraint.
ALTER TABLE payments
    ADD CONSTRAINT payments_status_known
        CHECK (status IN ('RECEIVED', 'PROCESSING', 'PROCESSED', 'FAILED', 'POISONED', 'HELD', 'REJECTED'));
