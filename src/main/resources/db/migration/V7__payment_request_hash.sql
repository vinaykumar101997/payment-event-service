-- SHA-256 (hex) of the POST /payments payload that created the row. A later POST with the
-- same payment_id is a replay only if its hash matches; otherwise it's rejected with 422.
-- Nullable: rows created before this migration have none, and are compared by recomputing
-- the hash from their stored columns instead (see Payment.matchesRequestHash).
ALTER TABLE payments ADD COLUMN request_hash VARCHAR(64);
