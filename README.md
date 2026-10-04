# payment-event-service

A payment processing service demonstrating REST + Kafka + Postgres with correctness
under concurrency: idempotent event consumption, pessimistic-locked atomic debit/credit,
business failures published as `payment.failed` events, and dead-letter handling for real
faults (untrusted events, poison payments).

## Architecture

```
Client
  |  POST /payments
  v
PaymentController --validates(structural)--> PaymentService.createPayment()
  |                                              (RECEIVED row + "payments" outbox row,
  |  GET /payments/{id}, GET /accounts/{id}        one transaction)
  |  (read from Postgres)
  v
(response)

OutboxRelay (every 500ms): claim pending outbox rows (FOR UPDATE SKIP LOCKED)
  -> PaymentProducer -> "payments" / "transactions" topic -> mark published
  (failed publish: row backed off and retried, never dropped)

"payments" topic (key = paymentId, 3 partitions)
  |
  v
PaymentConsumer -> PaymentService.process()
  |-- claim (RECEIVED -> PROCESSING, atomic UPDATE)
  |-- compare event to the stored Payment row (mismatch -> mark FAILED, rethrow, no money moved)
  |-- lock both accounts (PESSIMISTIC_WRITE, sorted order)
  |-- validate existence/currency/balance (under the lock, from the stored row)
  |-- debit/credit + ledger entries +       --or--   business rule failed: mark FAILED +
  |   "transactions" outbox row, PROCESSED             "payment.failed" outbox row (no DLT)
  v
(OutboxRelay publishes to "transactions" / "payment.failed")

Non-business failure (DB error, ...): attempt counted, Kafka retries with backoff;
  after payments.processing.max-attempts -> POISONED (terminal) -> "payments.DLT".
Event/stored-row mismatch: mark FAILED + "payment.failed", and -> "payments.DLT".

PaymentSweeper (every 30s): re-enqueues (via the outbox) anything still RECEIVED after 60s
(safety net for payments whose processing kept failing with retryable errors -
see "Known limitations" below)
```

## Running locally

```bash
docker-compose up --build
```

This starts Postgres, a single-node KRaft-mode Kafka broker, and the app on `:8080`.

```bash
curl -X POST localhost:8080/payments -H "Content-Type: application/json" -d '{
  "paymentId": "p-1", "fromAccount": "ACC-1001", "toAccount": "ACC-1002",
  "amount": 25.00, "currency": "USD"
}'
curl localhost:8080/payments/p-1
curl localhost:8080/accounts/ACC-1001
```

(`ACC-1001` / `ACC-1002` are seeded by `V2__seed_demo_accounts.sql` for local convenience.)

## Running tests

```bash
./mvnw test      # unit tests only, no Docker required
./mvnw verify    # unit + Testcontainers integration tests (needs Docker running)
```

Integration tests spin up real Postgres and Kafka containers per test JVM (shared across
test classes via a common base class) and exercise the full REST -> Kafka -> consumer ->
Postgres round trip, including:
- a payment processed end-to-end with correct balance changes,
- a duplicate POST rejected (409) without double-debiting,
- a POST with an unknown `fromAccount` or `toAccount` rejected with 422 and an
  `unknown-account` problem (no payment row created),
- an insufficient-funds payment failing and publishing `payment.failed`, not landing on
  `payments.DLT`,
- a Kafka event whose amount disagrees with the stored payment row failing the payment,
  moving no money, and landing on `payments.DLT`,
- a stale `RECEIVED` payment (backdated past the 60s threshold) re-enqueued by a direct
  `PaymentSweeper.sweep()` call and processed exactly once,
- 100 concurrent payments against one account where only 60 have covering funds (asserts
  exactly 60 PROCESSED / 40 FAILED and a final balance of exactly zero, never negative),
- concurrent transfers in both directions between the same two accounts (asserts total
  money conserved, which only holds if the deadlock-avoiding lock order is correct).

## Key design decisions

- **Idempotency, two layers.** A duplicate POST is caught by the `payment_id` primary key.
  It's only a replay if the payload matches too - a SHA-256 of the request is stored in
  `payments.request_hash` (amount normalized, so `100` and `100.00` match). An identical
  retry gets **409** with a `duplicate-payment` problem carrying the existing payment (and
  its current status) in a `payment` member; the same `paymentId` with a different payload
  gets **422** `payment-id-reused`. A duplicate Kafka delivery of an
  already-terminal payment is caught by an atomic conditional `UPDATE ... WHERE status =
  'RECEIVED'` in `PaymentRepository.claimForProcessing` - the row count from that single
  statement is the idempotency signal, with no separate check-then-act race window.
- **Duplicate vs unknown account on POST.** The `payments` insert can fail on the primary
  key (duplicate `paymentId`) or on the `from_account`/`to_account` foreign keys (unknown
  account). `PaymentService.createPayment()` tells them apart by the SQLState in the
  exception's cause chain - `23505` (unique violation) becomes `DuplicatePaymentException`
  (409, or 422 if the payload differs - see above), `23503` (foreign key violation) becomes
  `UnknownAccountException` (422 `unknown-account` problem); anything else is rethrown
  untouched.
- **Errors are RFC 9457 problem details.** Every API error is `application/problem+json`
  with a stable `type` (`https://example.com/problems/<slug>`) that clients can branch on:
  400 `validation-failed` (with per-field `errors`), `malformed-request`, `invalid-request`,
  `invalid-amount`; 404 `not-found`; 409 `duplicate-payment`; 422 `unknown-account`,
  `payment-id-reused`.
- **The stored `Payment` row, not the Kafka event, drives money movement.** After a
  successful claim, `process()` locks, validates, and debits/credits using only the loaded
  row's accounts, amount, and currency. If the event's fields disagree with the row, no
  money moves: the payment is marked `FAILED` with the differing fields in
  `failureReason`, an error is logged, and a non-retryable `PaymentEventMismatchException`
  sends the message to `payments.DLT`.
- **Pessimistic locking over optimistic.** A hot account being hit by many concurrent
  payments is the expected case here, not a rare collision - `SELECT ... FOR UPDATE`
  serializes access directly instead of paying for retry storms. Both accounts are always
  locked in the same sorted order regardless of transfer direction, so a concurrent A->B
  and B->A transfer can never deadlock against each other.
- **Validation happens under the lock, not before it.** Checking balance/currency before
  acquiring the lock would leave a window for the balance to change between the check and
  the debit. `PaymentService.process()` locks first, then validates.
- **Events go through a transactional outbox.** `createPayment()` writes the `payments`
  event, and `process()` writes the `transactions` event, to `outbox_events` in the same
  transaction as the state change they describe - so a committed payment always has its
  event, and a rolled-back one never does. Neither the controller nor the consumer
  publishes to Kafka. `OutboxRelay` claims pending rows with `SELECT ... FOR UPDATE SKIP
  LOCKED` (safe with multiple instances), publishes them, and marks them published; a
  failed publish is recorded and retried with exponential backoff. The relay is the one
  place Kafka I/O happens inside a DB transaction, and it only holds locks on the claimed
  outbox rows - never on account or payment rows - so a slow broker can delay publishing
  but can't block a payment. Delivery is at-least-once, so consumers dedupe by `paymentId`.
- **Business failures are outcomes, not faults.** Insufficient funds, currency mismatch,
  and unknown account mark the payment `FAILED` and write a `payment.failed` event to the
  outbox in the same transaction; `process()` returns normally, so nothing is retried and
  nothing reaches the DLT. The DLT is for real faults only, and routing to it is
  exception-driven: an event/stored-row mismatch (`PaymentEventMismatchException`) and a
  payment that just hit its attempt limit (`PaymentPoisonedException`) are non-retryable;
  anything else (DB blips, etc.) retries with backoff first. All go through the same
  `DeadLetterPublishingRecoverer`, targeting `<topic>.DLT` - the framework's own default
  naming convention.

## Known limitations / production follow-ups

- **`PaymentSweeper` is a safety net, not the delivery mechanism.** The outbox closes the
  POST -> Kafka dual-write gap, but a payment can still be stranded in `RECEIVED`: if its
  processing keeps failing with a *retryable* error (e.g. the database unreachable for
  longer than the listener's ~10s backoff), the claim rolls back and the message goes to
  `payments.DLT`, and nothing would process it again. Every 30 seconds the sweeper
  re-enqueues (through the outbox, so the relay stays the only publisher) anything still
  `RECEIVED` after 60 seconds (this also covers `RECEIVED` rows created before the outbox
  migration). It claims with `UPDATE ... WHERE payment_id IN (SELECT ... FOR UPDATE SKIP
  LOCKED) RETURNING`, so multiple instances never hand off the same payment, and enqueues
  each one in its own transaction so one failure doesn't abort the batch.
- **The outbox table grows without bound.** Published rows are kept (useful for auditing
  and debugging) but nothing deletes them yet; production would need a retention job.
  A CDC relay (e.g. Debezium on the outbox table's WAL) would also replace the polling
  relay's 500ms latency floor and per-poll queries.
- **Per-key ordering is not guaranteed once a payment can have more than one event on the
  same topic.** Today each payment has at most one event per topic (`payments`,
  `transactions`, `payment.failed`), so ordering never matters. But outbox rows are retried
  independently, each with its own backoff: if a payment ever gets two events on one topic
  (e.g. a future reversal after a `transactions` event), a failed first publish can be
  overtaken by the second, and consumers would see them out of order even though they share
  a key and partition. Adding such events would need per-key ordering in the relay (e.g.
  don't claim a row while an older row with the same key is still pending) or consumers that
  order by a sequence number.
