# payment-event-service

[![CI](https://github.com/vinaykumar101997/payment-event-service/actions/workflows/ci.yml/badge.svg)](https://github.com/vinaykumar101997/payment-event-service/actions/workflows/ci.yml)

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

1. The client sends the payment to the REST API. The service validates it and saves it with the status RECEIVED. (With screening switched on it is saved as HELD instead and waits for a screening verdict or an officer; see [Screening hold](#screening-hold).)
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

75 unit tests and 72 integration tests in total (see Verification).

## Design decisions

**Asking twice never pays twice.** If the same payment request arrives again, the service recognises it and returns the original result.
*Technical:* the payment id is the primary key. A SHA-256 hash of the request separates a genuine retry (409) from a reused id with different details (422). On the Kafka side, a conditional `UPDATE ... WHERE status = 'RECEIVED'` claims each payment exactly once.

**The database is the source of truth.** The processor trusts the saved payment, not the message that triggered it, so a corrupted or tampered message can't change the amount.
*Technical:* after claiming, `process()` uses only the stored row. If the message disagrees with it, the payment fails and the message goes to the dead-letter topic.

**One payment at a time per account.** When two payments reach the same account together, one waits for the other, so every balance check is accurate.
*Technical:* both accounts are locked with a JPA pessimistic write lock, which Hibernate sends to Postgres as `SELECT ... FOR NO KEY UPDATE`, always in sorted order so opposite transfers can't deadlock. Validation happens after the lock is taken, never before. `FOR NO KEY UPDATE` (rather than `FOR UPDATE`) leaves the foreign-key check of a new payment for the same account unblocked.

**Books that always balance.** Every transfer writes a matching debit and credit, and a reconciliation check confirms the totals line up. The database itself refuses a negative balance.
*Technical:* `ledger_entries` are written in the same transaction as the balance change. CHECK constraints enforce `balance >= 0`, `amount > 0` and different source and destination accounts.

**No lost notifications.** The note telling other systems about a payment is saved together with the payment itself, so either both happen or neither does.
*Technical:* transactional outbox. A relay claims rows with `FOR UPDATE SKIP LOCKED`, publishes them and retries failures with backoff. Delivery is at-least-once, so consumers deduplicate by payment id.

**A declined payment is not a crash.** Insufficient funds is a normal answer and is reported as a failed payment. Only genuine faults go to a separate "needs attention" queue.
*Technical:* business failures publish `payment.failed`. The dead-letter topic receives only mismatched events and payments that exceed the retry limit (they are marked POISONED).

**Nothing waits forever, and amounts make sense.** A stuck database lock fails fast instead of freezing the service, and amounts are checked against each currency's rules.
*Technical:* Postgres `lock_timeout` and `statement_timeout` are set for application connections. Decimal places are validated per currency (JPY has none) using `java.util.Currency`, and the maximum amount is configurable per currency. Database migrations run on their own connection without the statement timeout, so a long migration isn't cancelled at start-up.

**Clear, consistent errors.** Every error response has the same shape, so client applications can handle them reliably.
*Technical:* RFC 9457 problem details (`application/problem+json`) with a stable `type` per error, each resolving to its entry in [docs/problems.md](docs/problems.md).

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
export PAYMENTS_API_KEY=$(openssl rand -hex 32)   # any value; clients must send it
docker-compose up --build
```

This starts Postgres, a single-node Kafka broker and the app on port 8080. Accounts `ACC-1001` and `ACC-1002` are created for you. Compose refuses to start the app if `PAYMENTS_API_KEY` is not set; no key is stored in the repository.

```bash
curl -X POST localhost:8080/payments -H "X-API-Key: $PAYMENTS_API_KEY" -H "Content-Type: application/json" -d '{
  "paymentId": "p-1", "fromAccount": "ACC-1001", "toAccount": "ACC-1002",
  "amount": 25.00, "currency": "USD"
}'
curl -H "X-API-Key: $PAYMENTS_API_KEY" localhost:8080/payments/p-1
curl -H "X-API-Key: $PAYMENTS_API_KEY" localhost:8080/accounts/ACC-1001
```

To run the app from your IDE instead, start Postgres and Kafka with docker-compose and set `DB_PASSWORD` (the compose file uses `local-dev-only-not-a-secret`) and `PAYMENTS_API_KEY`. The app does not start without `PAYMENTS_API_KEY`.

## Running tests

```bash
./mvnw test      # unit tests, no Docker needed
./mvnw verify    # unit + integration tests (Docker must be running)
```

The integration tests start real Postgres and Kafka containers once per test run and share them across test classes.

## Verification

Last full run: 2026-10-15, `./mvnw -B clean verify` on branch `fix/payment-stage2` (commit a8dbd7f), Windows 11, JDK 17.0.12, Docker 29.8.1:

```
[INFO] Tests run: 75, Failures: 0, Errors: 0, Skipped: 0   (unit, surefire)
[INFO] Tests run: 72, Failures: 0, Errors: 0, Skipped: 0   (integration, failsafe)
[INFO] BUILD SUCCESS
[INFO] Total time:  02:36 min
```

At the end of Stage 1 (commit a112f52) the same command gave 49 unit and 44 integration tests in 03:11 min. Before Stage 1, on a clean checkout of `main` (commit 964449f), it gave 36 unit and 38 integration tests, 0 failures, in 02:38 min.

The badge above shows the latest GitHub Actions run of the same command.

## Screening hold

Optional fraud screening by a separate service (the plan is a Python service; it is not in this repository). Off by default (`payments.screening.enabled=false`); with it off, everything above is unchanged, which `ScreeningDisabledEndToEndIT` checks. When it is on, a new payment is saved as HELD and the service publishes a `payment.screening.requested` event instead of the `payments` event, so nothing can move its money until a screening verdict or a compliance officer releases it. Nothing ever releases a payment automatically. The event formats are JSON Schemas (draft 2020-12) in [`contracts/`](contracts/README.md), shared with the screening service; incoming verdicts are validated against them.

### Payment status transitions

| From | Trigger | To | Also |
|---|---|---|---|
| (new) | `POST /payments`, screening off | `RECEIVED` | outbox `payments` |
| (new) | `POST /payments`, screening on | `HELD` | outbox `payment.screening.requested` |
| `HELD` | verdict CLEAR | `RECEIVED` | outbox `payments` |
| `HELD` | officer release | `RECEIVED` | outbox `payments`; officer, note, time in `officer_decisions` |
| `HELD` | verdict FLAG or ERROR | `HELD` | none |
| `HELD` | verdict REJECT | `REJECTED` | outbox `payment.failed` |
| `HELD` | officer reject | `REJECTED` | outbox `payment.failed`; officer, note, time in `officer_decisions` |
| `HELD` | held longer than `payments.screening.timeout` (default 1 hour) | `HELD` | `overdue_at` set once, WARN log line, `HeldPaymentOverdueHook` called; never released or rejected |
| `RECEIVED` | consumer claims it | `PROCESSING` | none |
| `PROCESSING` | money moved | `PROCESSED` | ledger entries, outbox `transactions` |
| `PROCESSING` | business rule failed (funds, currency, account), or the event doesn't match the stored row | `FAILED` | outbox `payment.failed` |
| `RECEIVED` | `payments.processing.max-attempts` non-business failures | `POISONED` | message to `payments.DLT` |

Every verdict event is stored in `screening_verdicts` (migration V9) under the key (paymentId, eventId), with what it did. A redelivered event changes nothing. A verdict for a payment that has already left `HELD` is stored as `IGNORED_NOT_HELD` and not applied, so a payment is never decided twice. A verdict that doesn't match the schema, or names an unknown payment, goes to `payment.screening.completed.DLT` unchanged and is never applied.

Officer decisions (migration V11) are stored once per payment with officer, note and time. Repeating the same decision returns 200 and changes nothing (the first officer and note stay). The opposite decision afterwards, or a decision on a payment a verdict already decided, is 409 `payment-not-held`. A verdict and an officer decision on the same payment lock the payment row, so they are applied one after the other.

`PROCESSED`, `FAILED`, `POISONED` and `REJECTED` are terminal. From `RECEIVED` on, a released payment follows exactly the same path as one created with screening off. Postgres also enforces the list of statuses (constraint `payments_status_known`, migration V8).

### Listing held payments

`GET /payments?status=HELD` is the officers' work list. Items come oldest first (by `heldSince`, ties by `paymentId`):

```json
{
  "items": [
    {
      "paymentId": "p-2", "amount": 10.00, "currency": "USD",
      "fromAccount": "ACC-1001", "toAccount": "ACC-1002",
      "heldSince": "2026-10-15T12:00:00.123456Z", "overdue": false,
      "reasons": ["name match"], "riskScore": null
    }
  ],
  "nextCursor": null
}
```

`reasons` and `riskScore` come from the latest recorded verdict for the payment, and are both `null` if it has none. `overdue=true` returns only payments marked overdue, `overdue=false` only the others. Pagination uses a cursor: pass `nextCursor` back as `cursor` to get the next page; `null` means there are no more. The cursor holds the last item's position (`heldSince`, `paymentId`), so a payment released between two requests doesn't make the next page skip or repeat anything.

### Topics

| Topic | Direction | Key | Content |
|---|---|---|---|
| `payment.screening.requested` | published (outbox) | paymentId | [`payment.screening.requested.v1`](contracts/payment.screening.requested.v1.schema.json): schemaVersion, eventId, paymentId, accounts, amount as a decimal string, currency, createdAt |
| `payment.screening.completed` | consumed | paymentId | [`payment.screening.completed.v1`](contracts/payment.screening.completed.v1.schema.json): verdict CLEAR, FLAG, REJECT or ERROR, reasons, optional riskScore, decidedAt, screeningRunId. Read as plain JSON; no Spring type headers needed |
| `payment.screening.completed.DLT` | published | as received | verdict records that failed the schema or name an unknown payment, byte for byte; also valid verdicts that still failed after about 10 seconds of retries |

The three topics are created (3 partitions each) only when screening is on. `payments`, `transactions`, `payment.failed` and `payments.DLT` are unchanged.

### Running with screening on

Settings (environment variable form in brackets):

- `payments.screening.enabled` (`PAYMENTS_SCREENING_ENABLED`): `true` to hold new payments. Default `false`.
- `payments.screening.timeout` (`PAYMENTS_SCREENING_TIMEOUT`): ISO-8601 duration after which a HELD payment is marked overdue. Default `PT1H`.
- `payments.screening.overdue-sweep-interval-ms`: how often the overdue check runs. Default 60000.

Without the screening service you can play its part with Kafka's console producer. These steps were run end to end on 2026-10-15 (Windows 11, Git Bash, Docker Compose v5.5.1); the full transcript is in the [project book](docs/PROJECT_BOOK.md#demo-walkthrough). Run them all in one terminal: the compose file requires `PAYMENTS_API_KEY` for every `docker-compose` command, not only `up`.

```bash
export PAYMENTS_API_KEY=$(openssl rand -hex 32) PAYMENTS_SCREENING_ENABLED=true
docker-compose up --build -d           # wait for "Started PaymentEventServiceApplication" in: docker-compose logs app
# Git Bash on Windows only: stop it rewriting /opt/... paths in the docker-compose exec commands below
export MSYS_NO_PATHCONV=1

# 1. A payment that screening clears
curl -s -X POST localhost:8080/payments -H "X-API-Key: $PAYMENTS_API_KEY" -H "Content-Type: application/json" \
  -d '{"paymentId": "demo-clear", "fromAccount": "ACC-1001", "toAccount": "ACC-1002", "amount": 10.00, "currency": "USD"}'
# -> "status":"HELD"; the screening request is on payment.screening.requested:
docker-compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic payment.screening.requested --from-beginning --property print.key=true --property key.separator="|" --max-messages 1 --timeout-ms 10000
echo 'demo-clear|{"schemaVersion":1,"eventId":"4f3c2b1a-0e9d-4c8b-9a7f-6e5d4c3b2a10","paymentId":"demo-clear","verdict":"CLEAR","reasons":[],"riskScore":0.04,"decidedAt":"2026-10-15T12:00:00Z","screeningRunId":"manual-1"}' \
  | docker-compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 \
      --topic payment.screening.completed --property parse.key=true --property key.separator="|"
sleep 3                                # processing is asynchronous: RECEIVED first, PROCESSED once the consumer has run
curl -s -H "X-API-Key: $PAYMENTS_API_KEY" localhost:8080/payments/demo-clear          # -> "status":"PROCESSED"

# 2. Two payments that screening flags, then an officer releases one and rejects the other
curl -s -X POST localhost:8080/payments -H "X-API-Key: $PAYMENTS_API_KEY" -H "Content-Type: application/json" \
  -d '{"paymentId": "demo-release", "fromAccount": "ACC-1001", "toAccount": "ACC-1002", "amount": 20.00, "currency": "USD"}'
curl -s -X POST localhost:8080/payments -H "X-API-Key: $PAYMENTS_API_KEY" -H "Content-Type: application/json" \
  -d '{"paymentId": "demo-reject", "fromAccount": "ACC-1001", "toAccount": "ACC-1002", "amount": 30.00, "currency": "USD"}'
printf '%s\n' \
  'demo-release|{"schemaVersion":1,"eventId":"5a4d3c2b-1f0e-4d9c-8b7a-6f5e4d3c2b11","paymentId":"demo-release","verdict":"FLAG","reasons":["name match"],"riskScore":0.71,"decidedAt":"2026-10-15T12:01:00Z","screeningRunId":"manual-2"}' \
  'demo-reject|{"schemaVersion":1,"eventId":"6b5e4d3c-2a1f-4e0d-9c8b-7a6f5e4d3c12","paymentId":"demo-reject","verdict":"FLAG","reasons":["high-risk corridor"],"riskScore":0.88,"decidedAt":"2026-10-15T12:01:00Z","screeningRunId":"manual-2"}' \
  | docker-compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 \
      --topic payment.screening.completed --property parse.key=true --property key.separator="|"
sleep 3
curl -s -H "X-API-Key: $PAYMENTS_API_KEY" "localhost:8080/payments?status=HELD"       # both, with reasons and riskScore
curl -s -X POST localhost:8080/payments/demo-release/release -H "X-API-Key: $PAYMENTS_API_KEY" \
  -H "Content-Type: application/json" -d '{"officer": "alice", "note": "checked, not the listed person"}'
curl -s -X POST localhost:8080/payments/demo-reject/reject -H "X-API-Key: $PAYMENTS_API_KEY" \
  -H "Content-Type: application/json" -d '{"officer": "carol", "note": "customer confirmed fraud"}'
sleep 3
curl -s -H "X-API-Key: $PAYMENTS_API_KEY" localhost:8080/payments/demo-release        # -> "status":"PROCESSED"
curl -s -H "X-API-Key: $PAYMENTS_API_KEY" localhost:8080/payments/demo-reject         # -> "status":"REJECTED"

docker-compose down -v                 # stop and delete the demo data
```

The console producer reads verdicts from standard input, one `key|json` per line; `-T` lets `docker-compose exec` take the piped input. The consumer step prints the one screening request on a fresh stack (`--max-messages 1`); on a stack with older payments, drop `--max-messages 1` and it stops after 10 seconds with a `TimeoutException` message, which is expected. Status changes after a verdict or a release are asynchronous (outbox relay, then the payments consumer), hence the `sleep 3` lines.

### Not done yet

- **No screening service here.** This repository only publishes requests, consumes verdicts and holds payments; the Python screening service is separate and not built.
- **Overdue is a log line only.** `HeldPaymentOverdueHook` has no implementation (metrics are out of scope), so the WARN line is the only signal.
- **Dead-lettered verdicts are not replayed.** A verdict on `payment.screening.completed.DLT` stays there; its payment stays HELD until another verdict or an officer decides.
- **A lost screening request is not re-sent.** If its outbox row goes dead, the payment stays HELD and only shows up as overdue.
- **Officers are not individually authenticated.** Anyone with the API key can release or reject, and `officer` is free text.
- **Switching screening on doesn't screen older payments.** Only payments created while it is on are held; switching it off doesn't release HELD ones (verdicts are then not consumed, officers can still decide).
- **`riskScore` is stored, never used.**

## API

| Method | Path | Purpose |
|---|---|---|
| POST | `/payments` | Submit a payment |
| GET | `/payments/{id}` | Payment status and failure reason |
| GET | `/payments?status=HELD` | Held payments, oldest first. Optional `overdue=true` or `false`, `limit` (1 to 200, default 50), `cursor` |
| GET | `/accounts/{id}` | Account balance |
| POST | `/payments/{id}/release` | Officer releases a HELD payment. Body `{"officer": "...", "note": "..."}`, both required |
| POST | `/payments/{id}/reject` | Officer rejects a HELD payment. Same body |

Every request needs the header `X-API-Key` with the value of `PAYMENTS_API_KEY` (required, no default). The comparison is constant-time (SHA-256 digests compared with `MessageDigest.isEqual`). Only `/actuator/health` is left open; this build has no actuator, so that path returns 404, not 401.

Errors are `application/problem+json`: 401 `unauthorized` (missing or wrong key; same answer for both); 400 `invalid-query` (list parameters), `validation-failed`, `malformed-request`, `invalid-request`, `invalid-amount`; 404 `not-found`; 409 `duplicate-payment` (includes the existing payment), `payment-not-held` (officer decision on a payment that isn't HELD; includes it); 422 `unknown-account`, `payment-id-reused`; 503 `service-busy` (with `Retry-After`).

Each problem `type` is `payments.problem-base-uri` + the slug. By default that resolves to the matching section of [docs/problems.md](docs/problems.md) (for example `https://github.com/vinaykumar101997/payment-event-service/blob/main/docs/problems.md#invalid-amount`), which gives the status, when it occurs, an example body and what the client should do. Set `PAYMENTS_PROBLEM_BASE_URI` to publish them elsewhere. **Change:** `type` values used to be `https://example.com/problems/<slug>`; clients that matched on those must switch to the new URIs. There are no known clients.

## Known limitations

- **The sweeper is a safety net, not the main delivery path.** If processing keeps failing with a retryable error for longer than the consumer's retries, the message goes to the dead-letter topic and the payment stays RECEIVED. The sweeper re-enqueues such payments after 60 seconds.
- **A payment on a permanently locked account never finishes and is never poisoned.** A lock timeout (Postgres `lock_timeout`, SQLState 55P03) is retried but not counted toward the poison limit, so a busy account can't push a valid payment to POISONED. The flip side: if the account stays locked, the payment cycles through Kafka retries, the dead-letter topic and the sweeper until the lock is released. Each lock timeout is logged.
- **A payment can't be created while something else holds one of its accounts `FOR UPDATE`.** Inserting a payment checks its foreign keys, which needs a `FOR KEY SHARE` lock on both accounts; only `FOR UPDATE` blocks that. The service itself never takes `FOR UPDATE` on accounts (payment processing uses `FOR NO KEY UPDATE`), but a script or manual `SELECT ... FOR UPDATE` can. If the wait passes `lock_timeout`, the POST returns 503 `service-busy` with `Retry-After: 1`, and the client retries with the same `paymentId`.
- **Outbox retention deletes, it does not archive.** Published outbox rows older than `outbox.retention.days` (default 30) are deleted hourly in batches; dead and pending rows are kept. There is no archive, so the audit trail in `outbox_events` only goes back that far.
- **Limits are per payment and change only on restart.** `payments.max-amount-by-currency` sets a cap per currency (falling back to `payments.max-amount`). There are no per-account or per-day limits.
- **Migrations have no statement timeout.** Flyway runs on its own connection without `statement_timeout` (it keeps `lock_timeout`), so a runaway migration is stopped only by a lock timeout or an operator.
- **Problem type URIs point at `main` on GitHub by default.** A client reads the latest [docs/problems.md](docs/problems.md), not necessarily the version deployed. Set `PAYMENTS_PROBLEM_BASE_URI` per environment to change it.
- **One API key for every client.** All clients share `PAYMENTS_API_KEY`; there are no per-client keys or roles, so an officer decision can be made by any client, and `officer` in the request body is not verified. Changing the key needs a restart.
- **Event order per payment is not guaranteed once a payment can have two events on the same topic.** Today each payment has at most one event per topic, so it doesn't matter yet. Future reversals would need ordered publishing per key.

## Roadmap

- ISO 20022 messages: accept pacs.008, return pacs.002 status reports, generate camt.053 statements from the ledger
- Reversals using compensating ledger entries
- Metrics (Micrometer, Prometheus), OpenTelemetry tracing, payment id as a correlation id
- Load and chaos tests: throughput and p95/p99 latency with k6, a consumer killed mid-payment
- Per-client API keys or OAuth 2 client credentials, with an officer role for release and reject

## License

[MIT](LICENSE)
