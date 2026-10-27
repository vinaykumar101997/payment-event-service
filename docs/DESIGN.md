# Design

How payment-event-service works, why it is built this way, which bugs it has had, and how to
check it. The [README](../README.md) covers setup and the API; this document is for an engineer
changing the code.

## Overview

The service moves money between accounts and must stay correct when requests are repeated,
payments collide on one account, or Postgres or Kafka fail part-way. `POST /payments` validates
a payment and saves it as `RECEIVED` together with an outbox row, in one transaction. A relay
publishes outbox rows to Kafka. A consumer claims the payment, locks both accounts in sorted
order, checks the balance against the stored row, moves the money and writes two ledger
entries plus the outcome event's outbox row, again in one transaction. With screening on, a
new payment is saved as `HELD` instead and waits for a verdict from a separate screening
service, or for a compliance officer.

```
Client --POST /payments--> PaymentService.createPayment()
                             RECEIVED + outbox "payments"                (screening off)
                             HELD + outbox "payment.screening.requested" (screening on)

OutboxRelay (500 ms): claim rows FOR UPDATE SKIP LOCKED -> publish -> mark published

"payment.screening.completed" -> ScreeningVerdictConsumer -> schema check -> ScreeningService
officer POST /payments/{id}/release|reject ------------------------------> ScreeningService
   CLEAR / release: HELD -> RECEIVED + outbox "payments"
   REJECT / reject: HELD -> REJECTED + outbox "payment.failed"
   FLAG / ERROR:    stays HELD

"payments" (key = paymentId) -> PaymentConsumer -> PaymentService.process()
   claim RECEIVED -> PROCESSING, compare event to stored row, lock accounts (sorted),
   validate under the lock, then either
   debit/credit + 2 ledger entries + outbox "transactions"   -> PROCESSED
   or outbox "payment.failed"                                -> FAILED

Non-business failure: attempt counted, Kafka retries; at max-attempts -> POISONED -> payments.DLT
PaymentSweeper (30 s): re-enqueues payments still RECEIVED after 60 s
HeldPaymentSweeper (60 s): marks HELD payments older than payments.screening.timeout as overdue
```

Java 17, Spring Boot 3.5, Spring Data JPA (Hibernate 6.6), PostgreSQL 16 with Flyway
(migrations V1 to V11), Apache Kafka (KRaft) with Spring Kafka, Spring Security for the API key.
Tables: `accounts`, `payments`, `ledger_entries`, `outbox_events`, `screening_verdicts`,
`officer_decisions`.

## Design decisions

**Idempotency.** The client's `paymentId` is the primary key, so a second insert of the same
payment fails atomically in Postgres. A SHA-256 hash of the request tells an identical retry
(409 with the original payment) from a reused id with a different body (422). On the consumer
side, `UPDATE ... SET status = 'PROCESSING' WHERE status = 'RECEIVED'` claims each payment
exactly once, whatever redelivers the message (Kafka retry or the sweeper).

**The stored row is the source of truth.** After claiming, processing uses only the saved
payment, never the Kafka message body. A message that disagrees with the row fails the payment
and goes to `payments.DLT`, so a corrupted or tampered message can't change how much money
moves.

**Locking and ordering.** Both accounts are locked with JPA `PESSIMISTIC_WRITE`, which Hibernate
sends to Postgres as `SELECT ... FOR NO KEY UPDATE`, always in sorted id order so transfers in
opposite directions can't deadlock. The balance is checked after the lock is taken, so it can't
go stale between check and debit. `FOR NO KEY UPDATE` rather than `FOR UPDATE` leaves the
`FOR KEY SHARE` lock that a new payment's foreign-key check needs unblocked. Kafka keys every
message by `paymentId`; today each payment has at most one event per topic, so per-payment
ordering across events is not relied on.

**Transactional outbox.** Writing the database and publishing to Kafka are two operations, and
a crash between them loses one. Every event is written as an `outbox_events` row in the same
transaction as the state change it describes; `OutboxRelay` claims rows with
`FOR UPDATE SKIP LOCKED` (so several instances never publish the same row), publishes them and
retries failures with backoff. A row that keeps failing is marked dead after
`outbox.relay.max-attempts`. Delivery is at-least-once, so consumers deduplicate by payment id.
Published rows older than `outbox.retention.days` are deleted hourly in short batches.

**Ledger and reconciliation.** Every transfer writes a debit and a matching credit to
`ledger_entries` in the same transaction as the balance change. That makes two invariants
checkable: each payment's entries net to zero, and each balance equals its opening balance plus
its entries. `ReconciliationService` checks both; the integration tests run it after concurrent
workloads and after every screening scenario. CHECK constraints in Postgres enforce
`balance >= 0`, `amount > 0`, distinct accounts and the list of known statuses, so no script or
future code path can break them.

**Screening hold.** With `payments.screening.enabled=true`, a payment is saved `HELD` and only a
`payment.screening.requested` event leaves the service; the status and the outbox row are
chosen in the same transaction, so they always agree. A `HELD` payment is never claimed (the
claim needs `RECEIVED`) and never swept. Verdicts are validated against the JSON Schema in
`contracts/` and stored once per (paymentId, eventId) with `ON CONFLICT DO NOTHING` in the same
transaction as the transition, so a redelivered verdict does nothing. Verdicts and officer
decisions both lock the payment row and check `HELD` under the lock, so only the first decision
applies; release and reject call the same code as CLEAR and REJECT.

**Fail closed.** Nothing releases a held payment automatically: a payment held past
`payments.screening.timeout` is marked overdue and logged, never released or rejected, because
an automatic release would let anything through by slowing screening down. A verdict that fails
the schema or names an unknown payment goes to `payment.screening.completed.DLT` byte for byte
and is never applied. The schemas are loaded at start-up whether screening is on or not, so a
build without them fails immediately instead of at the first verdict. A declined payment
(insufficient funds, unknown account) is a normal `payment.failed` outcome; only real faults
reach a dead-letter topic.

**API key authentication.** Every path except `/actuator/health` needs `X-API-Key` equal to
`PAYMENTS_API_KEY`, which has no default, so a deployment can't start with a guessable key. The
presented and configured keys are hashed with SHA-256 and compared with `MessageDigest.isEqual`,
so response time doesn't depend on how much of a guess is right or on its length. The chain is
stateless (no session, no CSRF, no form login), and a missing or wrong key gets the same 401
`unauthorized` problem as every other error, rendered through `ApiExceptionHandler`.

**Timeouts and errors.** Application connections carry Postgres `lock_timeout` (5 s) and
`statement_timeout` (30 s), so a held row lock fails fast instead of pinning threads. Errors are
RFC 9457 problem details whose `type` resolves to a section of [problems.md](problems.md); a
test keeps the documented types and the returnable types identical.

## Failure cases and fixes

Bugs found in this codebase, each with the test that now guards it.

**Lock timeouts poisoned valid payments.**
*Symptom:* a valid payment for a busy account became `POISONED` after enough lock waits.
*Cause:* `PaymentConsumer` counted every non-business failure toward
`payments.processing.max-attempts`, including Postgres lock timeouts (SQLState `55P03`), which
mean "busy", not "broken".
*Fix:* the consumer looks for `55P03` in the cause chain and retries without counting; every
other error, including `statement_timeout` (`57014`), still counts.
*Tests:* `PaymentConsumerTest.lockTimeoutsAreRetriedButNeverCountedTowardThePoisonLimit`,
`PoisonPaymentIT.paymentThatOnlyHitsLockTimeoutsIsNeverPoisoned`.

**`POST /payments` returned a bare 500 while an account was locked.**
*Symptom:* creating a payment failed with 500 after 5 s when something outside the service held
one of its accounts `FOR UPDATE`.
*Cause:* inserting a payment runs a foreign-key check that takes `FOR KEY SHARE` on both
accounts; `FOR UPDATE` blocks that, and the resulting lock timeout was unmapped. The first
diagnosis blamed the consumer's own account lock; logging the SQL showed Hibernate already
sends `for no key update`, which doesn't block the check, so the consumer was never the cause.
*Fix:* `55P03` during creation becomes 503 `service-busy` with `Retry-After: 1`; nothing is
saved, and the client retries with the same `paymentId`.
*Tests:* `PaymentCreationUnderLockIT.postIsA503ServiceBusyProblemWhenTheAccountIsHeldForUpdatePastTheLockTimeout`,
`PaymentCreationUnderLockIT.postSucceedsImmediatelyWhileTheConsumerLockIsHeldOnTheAccount`
(guards the consumer's lock mode against a future Hibernate change),
`PaymentServiceTest.createPaymentTranslatesLockTimeoutToServiceBusy`.

**Migrations inherited the 30 s statement timeout.**
*Symptom:* any migration slower than 30 s would be cancelled at start-up on every instance.
*Cause:* Flyway borrowed a pooled connection, which carries the application's
`statement_timeout`.
*Fix:* `spring.flyway.url`, `user` and `password` give Flyway its own non-pooled connection
without `statement_timeout`; `init-sqls` keeps `lock_timeout` for the migration session.
*Test:* `DatabaseTimeoutsIT.migrationsRunWithoutStatementTimeoutWhileApplicationConnectionsKeepBothTimeouts`.

**The Docker image could not start.**
*Symptom:* `docker-compose up --build` built the image, then the app exited with
`IllegalStateException: Schema /contracts/payment.screening.completed.v1.schema.json is not on
the classpath`. Found by running the compose walkthrough by hand.
*Cause:* the pom reads `contracts/` as a resource directory, but the Dockerfile copied only
`pom.xml` and `src/`, and Maven skips a missing resource directory silently. The unit and
integration tests run on the checkout, where `contracts/` exists, and CI never builds the image.
*Fix:* `COPY contracts ./contracts` in the Dockerfile.
*Test:* `DockerfileTest.dockerBuildCopiesEveryResourceDirectoryThePomReads` (every
`<resource><directory>` in the pom needs a matching `COPY`).

**One maximum amount for every currency.**
*Symptom:* 5,000,000 was rejected in JPY (a modest amount) exactly as in USD.
*Cause:* a single `payments.max-amount`.
*Fix:* `payments.max-amount-by-currency.<CODE>` with `payments.max-amount` as the fallback;
unknown currency codes fail start-up. The default is quoted in `application.yml`, because
unquoted YAML read `1000000.00` as a float and error messages said `1000000.0`.
*Tests:* `MoneyRulesTest.sameAmountIsAcceptedInOneCurrencyAndRejectedInAnother`,
`ApiProblemDetailsIT.sameAmountIsAcceptedInJpyAndRejectedInUsd`.

**The outbox table grew without limit.**
*Cause:* published rows were never deleted.
*Fix:* `OutboxRetentionJob` deletes published rows older than `outbox.retention.days` (default
30) in batches with `FOR UPDATE SKIP LOCKED`; dead and pending rows are kept.
*Test:* `OutboxRetentionIT.deletesOldPublishedRowsInBatchesAndKeepsNewPublishedDeadAndPendingRows`.

**Problem types pointed at a placeholder.**
*Symptom:* every error `type` was `https://example.com/problems/<slug>`, which resolves to nothing.
*Fix:* `type` is `payments.problem-base-uri` plus the slug, by default a section of
[problems.md](problems.md).
*Test:* `ProblemTypesDocumentedTest.everyReturnableProblemTypeHasAHeadingAndEveryHeadingIsReturnable`
compares documented headings with returnable types in both directions.

**The compose walkthrough in the README was wrong in four places.** Running it by hand showed:
every `docker-compose` command, not only `up`, needs `PAYMENTS_API_KEY` set; Git Bash rewrites
`/opt/kafka/...` into a Windows path unless `MSYS_NO_PATHCONV=1`; a status check right after a
verdict shows `RECEIVED` because processing is asynchronous; and the console consumer's
`--timeout-ms` exit prints a `TimeoutException`. The walkthrough now uses one terminal,
`MSYS_NO_PATHCONV=1`, `sleep 3` before status checks and `--max-messages 1`.
*Test:* `ProjectDocsTest.screeningWalkthroughWasRunAndRecorded`.

The core design also guards these failure modes, each covered by an integration test:

| Failure mode | Guard | Test |
|---|---|---|
| Retry after a timeout charges twice | `paymentId` primary key, request hash | `PaymentFlowIT.duplicatePostIsRejectedAndDoesNotDoubleDebit`, `PaymentFlowIT.reusedPaymentIdWithDifferentPayloadIsRejectedWith422` |
| Overdraft under concurrency | lock both accounts, then validate | `ConcurrencyIT.partialFundsAllowsExactlySixtyOfOneHundredConcurrentPayments` |
| Deadlock on opposite transfers | lock in sorted id order | `ConcurrencyIT.concurrentTransfersInBothDirectionsConserveTotalMoney` |
| Event lost when Kafka is down | transactional outbox | `OutboxIT.paymentEventIsEventuallyPublishedWhenKafkaPublishFails`, `OutboxIT.transactionEventOfAProcessedPaymentSurvivesKafkaPublishFailures` |
| Tampered message changes the amount | stored row drives money movement | `PaymentFlowIT.eventThatDoesNotMatchStoredPaymentMovesNoMoneyAndGoesToDlt` |
| A failing payment retried forever | attempt counter, `POISONED` | `PoisonPaymentIT.paymentThatKeepsFailingIsPoisonedAfterMaxAttemptsInsteadOfRetriedForever` |
| An outbox row retried forever | dead after max attempts | `OutboxIT.rowThatKeepsFailingIsMarkedDeadAfterMaxAttemptsAndNotRetriedAgain` |
| Declines mixed with faults in the DLT | `payment.failed` event | `PaymentFlowIT.insufficientFundsFailsThePaymentAndPublishesPaymentFailedNotTheDlt` |
| A stuck lock freezes the consumer | `lock_timeout` | `DatabaseTimeoutsIT.lockWaitOnAHeldAccountRowFailsFastInsteadOfHanging` |
| Balance drifts from the ledger | reconciliation | `LedgerReconciliationIT.reconciliationReportsBalanceDriftAndOneSidedEntries` |
| A held payment settles before screening | `HELD` + screening request in one transaction | `ScreeningHoldIT.createdPaymentIsHeldAndOnlyAScreeningRequestIsPublished` |
| A redelivered or late verdict decides twice | (paymentId, eventId) key, `HELD` check under lock | `ScreeningVerdictIT.duplicateAndLateConflictingVerdictsAreHarmless` |
| A malformed verdict is applied | schema validation, DLT | `ScreeningVerdictIT.payloadThatFailsTheSchemaGoesToTheDeadLetterTopicUnchanged` |
| An overdue hold is released automatically | sweeper only marks | `HeldPaymentSweeperIT.overdueHeldPaymentIsMarkedAndReportedButStaysHeld` |
| An officer reverses an earlier decision | one decision per payment | `OfficerDecisionIT.theOppositeDecisionAfterwardsIsAConflict` |
| Screening code changes the default flow | flag off by default | `ScreeningDisabledEndToEndIT.withScreeningOffThePaymentFlowIsUnchanged` |

## Verification

```bash
./mvnw -B clean verify   # unit tests (surefire) and integration tests (failsafe); Docker must be running
./mvnw -B test           # unit tests only, no Docker
```

The integration tests start real Postgres and Kafka containers with Testcontainers once per run
and share them across test classes.

Last full run, 2026-10-15, `./mvnw -B clean verify` on Windows 11, JDK 17.0.12, Docker 29.8.1:

```
[INFO] Tests run: 81, Failures: 0, Errors: 0, Skipped: 0   (unit, surefire)
[INFO] Tests run: 78, Failures: 0, Errors: 0, Skipped: 0   (integration, failsafe)
[INFO] BUILD SUCCESS
[INFO] Total time:  03:21 min
```

## Demo walkthrough

Without the screening service you can play its part with Kafka's console producer, using the
settings in the README's [Running with screening on](../README.md#running-with-screening-on).
Run the commands in one terminal: the compose file requires `PAYMENTS_API_KEY` for every
`docker-compose` command, not only `up`.

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

The commands above were run end to end against `docker-compose` on 2026-10-15 (Windows 11,
Git Bash, Docker Compose v5.5.1, Docker 29.8.1) on a fresh stack, line by line; only a
wait-for-start-up loop was added. Docker build output and container status lines are removed,
and the API key appears only as `$PAYMENTS_API_KEY`.

```
$ export PAYMENTS_API_KEY=$(openssl rand -hex 32) PAYMENTS_SCREENING_ENABLED=true
$ docker-compose up --build -d
2026-10-15T22:39:14.220Z  INFO 1 c.e.p.PaymentEventServiceApplication     : Started PaymentEventServiceAp
$ export MSYS_NO_PATHCONV=1
$ curl -s -X POST localhost:8080/payments -H "X-API-Key: $PAYMENTS_API_KEY" -H "Content-Type: application/json" -d '{"paymentId": "demo-clear", "fromAccount": "ACC-1001", "toAccount": "ACC-1002", "amount": 10.00, "currency": "USD"}'
{"paymentId":"demo-clear","fromAccount":"ACC-1001","toAccount":"ACC-1002","amount":10.00,"currency":"USD","status":"HELD","failureReason":null,"createdAt":"2026-10-15T22:39:15.336901150Z","processedAt":null}
$ docker-compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic payment.screening.requested --from-beginning --property print.key=true --property key.separator="|" --max-messages 1 --timeout-ms 10000
demo-clear|{"schemaVersion":1,"eventId":"70b06970-6036-4357-a7b7-b04c305124d2","paymentId":"demo-clear","fromAccount":"ACC-1001","toAccount":"ACC-1002","amount":"10.00","currency":"USD","createdAt":"2026-10-15T22:39:15.336901150Z"}
Processed a total of 1 messages
$ echo 'demo-clear|{"schemaVersion":1,"eventId":"4f3c2b1a-0e9d-4c8b-9a7f-6e5d4c3b2a10","paymentId":"demo-clear","verdict":"CLEAR","reasons":[],"riskScore":0.04,"decidedAt":"2026-10-15T12:00:00Z","screeningRunId":"manual-1"}' | docker-compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic payment.screening.completed --property parse.key=true --property key.separator="|"
$ sleep 3
$ curl -s -H "X-API-Key: $PAYMENTS_API_KEY" localhost:8080/payments/demo-clear
{"paymentId":"demo-clear","fromAccount":"ACC-1001","toAccount":"ACC-1002","amount":10.00,"currency":"USD","status":"PROCESSED","failureReason":null,"createdAt":"2026-10-15T22:39:15.336901Z","processedAt":"2026-10-15T22:39:21.359059Z"}
$ curl -s -X POST localhost:8080/payments -H "X-API-Key: $PAYMENTS_API_KEY" -H "Content-Type: application/json" -d '{"paymentId": "demo-release", "fromAccount": "ACC-1001", "toAccount": "ACC-1002", "amount": 20.00, "currency": "USD"}'
{"paymentId":"demo-release",...,"status":"HELD",...}
$ curl -s -X POST localhost:8080/payments -H "X-API-Key: $PAYMENTS_API_KEY" -H "Content-Type: application/json" -d '{"paymentId": "demo-reject", "fromAccount": "ACC-1001", "toAccount": "ACC-1002", "amount": 30.00, "currency": "USD"}'
{"paymentId":"demo-reject",...,"status":"HELD",...}
$ printf '%s\n' 'demo-release|{..."verdict":"FLAG","reasons":["name match"],"riskScore":0.71,...}' 'demo-reject|{..."verdict":"FLAG","reasons":["high-risk corridor"],"riskScore":0.88,...}' | docker-compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic payment.screening.completed --property parse.key=true --property key.separator="|"
$ sleep 3
$ curl -s -H "X-API-Key: $PAYMENTS_API_KEY" "localhost:8080/payments?status=HELD"
{"items":[{"paymentId":"demo-release","amount":20.00,"currency":"USD","fromAccount":"ACC-1001","toAccount":"ACC-1002","heldSince":"2026-10-15T22:39:24.277293Z","overdue":false,"reasons":["name match"],"riskScore":0.71},{"paymentId":"demo-reject","amount":30.00,"currency":"USD","fromAccount":"ACC-1001","toAccount":"ACC-1002","heldSince":"2026-10-15T22:39:24.487362Z","overdue":false,"reasons":["high-risk corridor"],"riskScore":0.88}],"nextCursor":null}
$ curl -s -X POST localhost:8080/payments/demo-release/release -H "X-API-Key: $PAYMENTS_API_KEY" -H "Content-Type: application/json" -d '{"officer": "alice", "note": "checked, not the listed person"}'
{"paymentId":"demo-release",...,"status":"RECEIVED",...}
$ curl -s -X POST localhost:8080/payments/demo-reject/reject -H "X-API-Key: $PAYMENTS_API_KEY" -H "Content-Type: application/json" -d '{"officer": "carol", "note": "customer confirmed fraud"}'
{"paymentId":"demo-reject",...,"status":"REJECTED","failureReason":"Rejected by officer carol: customer confirmed fraud",...}
$ sleep 3
$ curl -s -H "X-API-Key: $PAYMENTS_API_KEY" localhost:8080/payments/demo-release
{"paymentId":"demo-release",...,"status":"PROCESSED","failureReason":null,"createdAt":"2026-10-15T22:39:24.277293Z","processedAt":"2026-10-15T22:39:30.769424Z"}
$ curl -s -H "X-API-Key: $PAYMENTS_API_KEY" localhost:8080/payments/demo-reject
{"paymentId":"demo-reject",...,"status":"REJECTED","failureReason":"Rejected by officer carol: customer confirmed fraud",...}
$ docker-compose down -v
```

`...` marks fields cut from the printed output; the full verdict JSON is in the commands
above. In an
earlier run with the same commands the app logged, among others:

```
Payment demo2-clear held for screening
Payment demo2-clear cleared by screening (event 4f3c2b1a-0e9d-4c8b-9a7f-6e5d4c3b2a10, run manual-1)
Payment demo2-clear processed: ACC-1001 -> ACC-1002 amount=10.00 USD
Payment demo2-release stays HELD after screening verdict FLAG (event 5a4d3c2b-1f0e-4d9c-8b7a-6f5e4d3c2b11): [name match]
Payment demo2-release: officer alice recorded RELEASE (checked, not the listed person)
Payment demo2-reject: officer carol recorded REJECT (customer confirmed fraud)
Payment demo2-release processed: ACC-1001 -> ACC-1002 amount=20.00 USD
```

The walkthrough has been run on Windows with Git Bash only.

## Known limitations

- **Officer names are self-asserted.** The `officer` in a release or reject request is whatever
  the caller sends; the only check is the shared API key, so `officer_decisions` records who
  said they decided, not who did. A real system would take the identity from an OpenID Connect
  token's `sub` claim, restrict decisions to an officer role and enforce four-eyes approval.
- **One shared API key**, no per-client keys or roles; changing it needs a restart. The key
  travels in a header, so TLS in front of the service is required in any real deployment.
- **A payment on a permanently locked account is never poisoned.** Lock timeouts are retried
  without counting, so it cycles through Kafka retries, the DLT and the sweeper until the lock
  is released.
- **No screening service in this repository**, and dead-lettered verdicts are not replayed: the
  payment stays `HELD` until another verdict or an officer decides. A screening request whose
  outbox row goes dead is not re-sent either; the payment only shows up as overdue.
- **Overdue is a log line only.** `HeldPaymentOverdueHook` has no implementation.
- **Outbox retention deletes, it does not archive.**
- **Limits are per payment** and change only on restart; no per-account or per-day limits.
- **Migrations have no statement timeout**; a runaway migration is stopped only by
  `lock_timeout` or an operator.
- **CI does not build or start the Docker image**; `DockerfileTest` guards the one known cause
  of a broken image.
- **Per-payment event ordering** is not guaranteed once a payment can have two events on the
  same topic; today each payment has at most one per topic.

## Production hardening

A real system would authenticate each officer through an identity provider with OpenID Connect (single sign-on, ideally with multi-factor), send a short-lived access token instead of a shared key, take the officer's identity from the token's `sub` claim rather than the request body, allow release and reject only to a compliance-officer role, and enforce four-eyes approval (the person who releases a high-risk payment is not the one who created or flagged it). Each decision record would then name a verified person.
