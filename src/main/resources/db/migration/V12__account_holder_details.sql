-- What a screening service needs to know about each side of a payment. Both are nullable:
-- accounts created before this migration have neither, and a screening request simply
-- leaves out what it doesn't have. country is an ISO 3166-1 alpha-2 code; the check keeps
-- anything else out, because the event contract would reject it downstream.
ALTER TABLE accounts
    ADD COLUMN holder_name VARCHAR(140),
    ADD COLUMN country     VARCHAR(2),
    ADD CONSTRAINT accounts_country_iso2 CHECK (country ~ '^[A-Z]{2}$');

UPDATE accounts SET holder_name = 'Alice Example', country = 'US' WHERE id = 'ACC-1001';
UPDATE accounts SET holder_name = 'Bob Example', country = 'GB' WHERE id = 'ACC-1002';
