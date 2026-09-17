# payment-event-service

A payment processing service demonstrating REST + Kafka + Postgres with correctness
under concurrency: idempotent event consumption, pessimistic-locked atomic debit/credit,
and dead-letter handling for both validation failures and transient infrastructure errors.

## Architecture

```
Client
  |  POST /payments
  v
PaymentController --validates(structural)--> PaymentService.createPayment() (RECEIVED)
  |                                                    |
  |  GET /payments/{id}, GET /accounts/{id}            v
  |  (read from Postgres)                    PaymentProducer -> "payments" topic
  v
(response)

"payments" topic (key = paymentId, 3 partitions)
  |
  v
PaymentConsumer -> PaymentService.process()
  |-- claim (RECEIVED -> PROCESSING, atomic UPDATE)
  |-- lock both accounts (PESSIMISTIC_WRITE, sorted order)
  |-- validate existence/currency/balance (under the lock)
  |-- debit/credit, mark PROCESSED         --or--   mark FAILED, rethrow
  |                                                    |
  v                                                    v
PaymentProducer -> "transactions" topic         Kafka error handler -> "payments.DLT"
(after the transaction commits)                 (validation failures: immediate;
                                                  transient errors: after retry/backoff)

PaymentSweeper (every 30s): republishes anything still RECEIVED after 60s
(stop-gap for the POST -> Kafka publish dual-write gap - see "Known limitations" below)
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
- a duplicate POST rejected without double-debiting,
- an insufficient-funds payment failing and landing on `payments.DLT`,
- 100 concurrent payments against one account where only 60 have covering funds (asserts
  exactly 60 PROCESSED / 40 FAILED and a final balance of exactly zero, never negative),
- concurrent transfers in both directions between the same two accounts (asserts total
  money conserved, which only holds if the deadlock-avoiding lock order is correct).

## Key design decisions

- **Idempotency, two layers.** A duplicate POST is rejected by the `payment_id` primary
  key (409, with the existing payment's current status). A duplicate Kafka delivery of an
  already-terminal payment is caught by an atomic conditional `UPDATE ... WHERE status =
  'RECEIVED'` in `PaymentRepository.claimForProcessing` - the row count from that single
  statement is the idempotency signal, with no separate check-then-act race window.
- **Pessimistic locking over optimistic.** A hot account being hit by many concurrent
  payments is the expected case here, not a rare collision - `SELECT ... FOR UPDATE`
  serializes access directly instead of paying for retry storms. Both accounts are always
  locked in the same sorted order regardless of transfer direction, so a concurrent A->B
  and B->A transfer can never deadlock against each other.
- **Validation happens under the lock, not before it.** Checking balance/currency before
  acquiring the lock would leave a window for the balance to change between the check and
  the debit. `PaymentService.process()` locks first, then validates.
- **Kafka I/O never happens inside a `@Transactional` method.** Both `createPayment()` and
  `process()` are pure DB transactions; the caller (controller or consumer) publishes to
  Kafka only after the transaction has returned/committed. This keeps a slow or
  unavailable broker from ever holding a DB row lock open.
- **DLT routing is exception-driven, not manually coded.** `PaymentValidationException` is
  registered as non-retryable with Spring Kafka's `DefaultErrorHandler`; anything else
  (DB blips, etc.) retries with backoff first. Both paths funnel through the same
  `DeadLetterPublishingRecoverer`, targeting `<topic>.DLT` - which is the framework's own
  default naming convention, not a name invented to match this project's spec.

## Known limitations / production follow-ups

- **The POST -> Kafka publish is not atomic with the DB insert.** `PaymentService.
  createPayment()` commits the `Payment(RECEIVED)` row in its own transaction; the
  publish to `payments` happens afterward, outside that transaction. If the process
  crashes (or the publish simply fails) between those two steps, the row exists but no
  event was ever sent. `PaymentSweeper` is a stop-gap for this: every 30 seconds it
  republishes anything still `RECEIVED` after 60 seconds.

  The correct production fix is a **transactional outbox**: write the event to an outbox
  table in the *same* transaction as the `Payment` insert (so the DB write and the intent
  to publish are atomic by construction), then have a separate relay - a CDC tool like
  Debezium reading the outbox table's changelog, or a dedicated poller - publish from the
  outbox to Kafka and mark rows as sent. That replaces "poll for the absence of a side
  effect after the fact" with an actual atomic write, and is the standard pattern for this
  class of dual-write problem. It's out of scope for this project but is the first thing
  to reach for before running this design in production.
- **The transactional-outbox gap on the consumer side is handled, but only partially
  automatically.** If the listener crashes after `process()` commits but before
  publishing to `transactions`, Kafka redelivers the message; `claimForProcessing` won't
  re-claim it (status is already `PROCESSED`), but `PaymentService` still returns
  `ALREADY_PROCESSED` so the listener retries just the publish. Consumers of `transactions`
  must therefore dedupe by `paymentId` - delivery is at-least-once, not exactly-once.
- **The sweeper assumes a single app instance.** Its claim mechanism (`markSwept` plus a
  time-based cooldown) has no cross-instance coordination; running multiple instances
  could result in the same stale payment being republished by more than one instance in
  the same window. Harmless (the consumer's idempotency check absorbs the duplicate), but
  wasteful. A `SELECT ... FOR UPDATE SKIP LOCKED`-based claim would fix this if the
  service were ever scaled horizontally.
