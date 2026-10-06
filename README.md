# payment-event-service

A payment service that moves money between accounts correctly, even when requests are repeated, payments collide, or part of the system fails.

## What it is

A client sends a payment request, for example "move $25 from account A to account B". The service records it, processes it in the background through Kafka, and keeps a ledger of every debit and credit, the same way a bank keeps its books. Other systems are told about each outcome through events.

## Why it's harder than it sounds

Taking money from one account and adding it to another looks simple. The hard part is everything that can go wrong around it:

- **The same request arrives twice.** A mobile app retries after a timeout. The customer must still be charged only once.
- **Two payments hit the same account at the same moment.** The balance must never drop below zero.
- **Something fails halfway.** The database is slow, Kafka is down, or a process crashes. Money must never leave one account without arriving in the other, and no payment may be lost.
- **A message is wrong.** A corrupted or tampered message must not be able to change how much money moves.

The service is built around these cases, and each one is covered by automated tests that run against a real Postgres database and a real Kafka broker.

## How a payment flows

1. The client sends the payment to the REST API. The service validates it and saves it with the status RECEIVED.
2. In the same database transaction, it writes a note that the payment needs processing. A background relay publishes that note to Kafka.
3. A consumer picks up the message, claims the payment, locks both accounts, checks the balance, moves the money and writes two ledger entries.
4. The result is published as an event: `transactions` when the payment succeeds, `payment.failed` when it is declined.
5. Unexpected errors are retried. A payment that keeps failing ends up in a dead-letter topic for investigation.

## Results

What the integration tests prove, against real Postgres and Kafka:

| Scenario | Outcome |
|---|---|
| 100 payments at once against one account that can cover only 60 | Exactly 60 succeed, 40 are declined, the balance ends at exactly 0 |
| Transfers in both directions between two accounts at the same time | No deadlocks, total money conserved |
| The same payment request sent twice | Charged once, the repeat gets 409 |
| Kafka publishing fails several times in a row | The event is still delivered, money moves exactly once |
| A message whose amount disagrees with the saved payment | No money moves, the payment is failed for investigation |
| Ledger reconciliation after concurrent workloads | Every payment nets to zero, every balance matches its entries |

36 unit tests and 38 integration tests in total.

## Design decisions

**Asking twice never pays twice.** If the same payment request arrives again, the service recognises it and returns the original result.
*Technical:* the payment id is the primary key. A SHA-256 hash of the request separates a genuine retry (409) from a reused id with different details (422). On the Kafka side, a conditional `UPDATE ... WHERE status = 'RECEIVED'` claims each payment exactly once.

**The database is the source of truth.** The processor trusts the saved payment, not the message that triggered it, so a corrupted or tampered message can't change the amount.
*Technical:* after claiming, `process()` uses only the stored row. If the message disagrees with it, the payment fails and the message goes to the dead-letter topic.

**One payment at a time per account.** When two payments reach the same account together, one waits for the other, so every balance check is accurate.
*Technical:* both accounts are locked with `SELECT ... FOR UPDATE`, always in sorted order so opposite transfers can't deadlock. Validation happens after the lock is taken, never before.

**Books that always balance.** Every transfer writes a matching debit and credit, and a reconciliation check confirms the totals line up. The database itself refuses a negative balance.
*Technical:* `ledger_entries` are written in the same transaction as the balance change. CHECK constraints enforce `balance >= 0`, `amount > 0` and different source and destination accounts.

**No lost notifications.** The note telling other systems about a payment is saved together with the payment itself, so either both happen or neither does.
*Technical:* transactional outbox. A relay claims rows with `FOR UPDATE SKIP LOCKED`, publishes them and retries failures with backoff. Delivery is at-least-once, so consumers deduplicate by payment id.

**A declined payment is not a crash.** Insufficient funds is a normal answer and is reported as a failed payment. Only genuine faults go to a separate "needs attention" queue.
*Technical:* business failures publish `payment.failed`. The dead-letter topic receives only mismatched events and payments that exceed the retry limit (they are marked POISONED).

**Nothing waits forever, and amounts make sense.** A stuck database lock fails fast instead of freezing the service, and amounts are checked against each currency's rules.
*Technical:* Postgres `lock_timeout` and `statement_timeout` are set for application connections. Decimal places are validated per currency (JPY has none) using `java.util.Currency`, with a configurable maximum amount.

**Clear, consistent errors.** Every error response has the same shape, so client applications can handle them reliably.
*Technical:* RFC 9457 problem details (`application/problem+json`) with a stable `type` per error.

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
(safety net for payments whose processing kept failing with retryable errors)
```

## Tech stack

| Area | Tools |
|---|---|
| Language and framework | Java 17, Spring Boot 3.5, Spring Data JPA (Hibernate) |
| Database | PostgreSQL, Flyway migrations |
| Messaging | Apache Kafka (KRaft mode), Spring Kafka, dead-letter topics |
| Testing | JUnit 5, Mockito, AssertJ, Awaitility, Testcontainers |
| Build and delivery | Maven wrapper, Docker, Docker Compose, GitHub Actions |

## Running locally

```bash
docker-compose up --build
```

This starts Postgres, a single-node Kafka broker and the app on port 8080. Accounts `ACC-1001` and `ACC-1002` are created for you.

```bash
curl -X POST localhost:8080/payments -H "Content-Type: application/json" -d '{
  "paymentId": "p-1", "fromAccount": "ACC-1001", "toAccount": "ACC-1002",
  "amount": 25.00, "currency": "USD"
}'
curl localhost:8080/payments/p-1
curl localhost:8080/accounts/ACC-1001
```

To run the app from your IDE instead, start Postgres and Kafka with docker-compose and set `DB_PASSWORD` (the compose file uses `local-dev-only-not-a-secret`).

## Running tests

```bash
./mvnw test      # unit tests, no Docker needed
./mvnw verify    # unit + integration tests (Docker must be running)
```

The integration tests start real Postgres and Kafka containers once per test run and share them across test classes.

## API

| Method | Path | Purpose |
|---|---|---|
| POST | `/payments` | Submit a payment |
| GET | `/payments/{id}` | Payment status and failure reason |
| GET | `/accounts/{id}` | Account balance |

Errors are `application/problem+json`: 400 `validation-failed`, `malformed-request`, `invalid-request`, `invalid-amount`; 404 `not-found`; 409 `duplicate-payment` (includes the existing payment); 422 `unknown-account`, `payment-id-reused`.

## Known limitations

- **The sweeper is a safety net, not the main delivery path.** If processing keeps failing with a retryable error for longer than the consumer's retries, the message goes to the dead-letter topic and the payment stays RECEIVED. The sweeper re-enqueues such payments after 60 seconds.
- **The outbox table grows without limit.** Published rows are kept for auditing and nothing deletes them yet. A change-data-capture relay such as Debezium would also remove the 500ms polling delay.
- **Event order per payment is not guaranteed once a payment can have two events on the same topic.** Today each payment has at most one event per topic, so it doesn't matter yet. Future reversals would need ordered publishing per key.
- **One maximum amount for every currency.** The same cap applies in USD and JPY.
- **Lock timeouts count toward the poison limit.** A very busy account could, in theory, push a valid payment to POISONED.
- **The statement timeout also applies to database migrations.** A slow migration on a large table could fail.
- **Problem `type` URIs use an `example.com` placeholder.**

## Roadmap

- ISO 20022 messages: accept pacs.008, return pacs.002 status reports, generate camt.053 statements from the ledger
- A fraud check before money moves, with a HELD state and release/reject endpoints
- Reversals using compensating ledger entries
- Metrics (Micrometer, Prometheus), OpenTelemetry tracing, payment id as a correlation id
- Load and chaos tests: throughput and p95/p99 latency with k6, a consumer killed mid-payment
- API authentication
- Fix the smaller limitations above: per-currency maximums, lock timeouts excluded from the poison limit, migrations exempt from the statement timeout, an outbox retention job

## License

[MIT](LICENSE)
