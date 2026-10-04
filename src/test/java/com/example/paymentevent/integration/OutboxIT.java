package com.example.paymentevent.integration;

import com.example.paymentevent.api.PaymentRequest;
import com.example.paymentevent.api.PaymentResponse;
import com.example.paymentevent.domain.Account;
import com.example.paymentevent.domain.OutboxEvent;
import com.example.paymentevent.domain.PaymentStatus;
import com.example.paymentevent.repository.AccountRepository;
import com.example.paymentevent.repository.OutboxEventRepository;
import com.example.paymentevent.repository.PaymentRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class OutboxIT extends AbstractIntegrationTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private AccountRepository accountRepository;

    @Autowired
    private PaymentRepository paymentRepository;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private FaultInjectingPaymentProducer faultInjectingPaymentProducer;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private TransactionTemplate transactionTemplate;

    @AfterEach
    void clearInjectedFaults() {
        faultInjectingPaymentProducer.clear();
    }

    @Test
    void createPaymentWritesExactlyOnePaymentEventToTheOutbox() {
        Account from = accountRepository.save(new Account(id("SRC"), new BigDecimal("1000.00"), "USD"));
        Account to = accountRepository.save(new Account(id("DST"), new BigDecimal("0.00"), "USD"));
        String paymentId = id("PAY");
        PaymentRequest request = new PaymentRequest(paymentId, from.getId(), to.getId(), new BigDecimal("100.00"), "USD");

        assertThat(post(request).getStatusCode()).isEqualTo(HttpStatus.CREATED);
        // No await: the row is committed by the same transaction as the Payment insert,
        // which has finished by the time POST returns.
        assertThat(outboxEventRepository.findByMessageKeyAndTopic(paymentId, "payments")).hasSize(1);

        // A duplicate POST rolls back its insert, and with it any outbox write.
        assertThat(restTemplate.postForEntity("/payments", request, String.class).getStatusCode())
                .isEqualTo(HttpStatus.CONFLICT);
        assertThat(outboxEventRepository.findByMessageKeyAndTopic(paymentId, "payments")).hasSize(1);
    }

    /**
     * The first three attempts to publish the "payments" event fail. Before the outbox, the
     * controller logged the one failure and left the payment RECEIVED until PaymentSweeper
     * noticed it a minute later; now the relay retries the outbox row until it succeeds.
     */
    @Test
    void paymentEventIsEventuallyPublishedWhenKafkaPublishFails() {
        Account from = accountRepository.save(new Account(id("SRC"), new BigDecimal("1000.00"), "USD"));
        Account to = accountRepository.save(new Account(id("DST"), new BigDecimal("0.00"), "USD"));
        String paymentId = id("PAY");
        faultInjectingPaymentProducer.failNextPublishes("payments", paymentId, 3);

        assertThat(post(new PaymentRequest(paymentId, from.getId(), to.getId(), new BigDecimal("100.00"), "USD"))
                .getStatusCode()).isEqualTo(HttpStatus.CREATED);

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                assertThat(paymentRepository.findById(paymentId).orElseThrow().getStatus())
                        .isEqualTo(PaymentStatus.PROCESSED));

        assertThat(faultInjectingPaymentProducer.remainingFailures("payments", paymentId)).isZero();
        // The relay marks the row published when its batch transaction commits, which can be
        // a moment after the consumer has already processed the message.
        OutboxEvent row = awaitPublished(paymentId, "payments");
        assertThat(row.getAttempts()).isEqualTo(4);
        assertThat(row.getLastError()).contains("topic=payments");
    }

    /**
     * The "transactions" publish for an already-PROCESSED payment fails five times in a row.
     * Before the outbox, the listener published it after process() committed: Kafka's error
     * handler retried the listener (~4 attempts in 10s) and then sent the message to the DLT,
     * so the money had moved but the transaction event was never published. Now the event is
     * an outbox row committed with the debit/credit, and the relay keeps retrying it.
     */
    @Test
    void transactionEventOfAProcessedPaymentSurvivesKafkaPublishFailures() {
        Account from = accountRepository.save(new Account(id("SRC"), new BigDecimal("1000.00"), "USD"));
        Account to = accountRepository.save(new Account(id("DST"), new BigDecimal("0.00"), "USD"));
        String paymentId = id("PAY");
        faultInjectingPaymentProducer.failNextPublishes("transactions", paymentId, 5);

        post(new PaymentRequest(paymentId, from.getId(), to.getId(), new BigDecimal("100.00"), "USD"));

        assertThat(observeRecordKey("transactions", paymentId, Duration.ofSeconds(40)))
                .as("transaction event for %s should eventually reach the topic", paymentId)
                .isTrue();

        assertThat(paymentRepository.findById(paymentId).orElseThrow().getStatus()).isEqualTo(PaymentStatus.PROCESSED);
        assertThat(awaitPublished(paymentId, "transactions").getAttempts()).isEqualTo(6);
        // Retrying the publish never retried the money movement.
        assertThat(accountRepository.findById(from.getId()).orElseThrow().getBalance()).isEqualByComparingTo("900.00");
        assertThat(accountRepository.findById(to.getId()).orElseThrow().getBalance()).isEqualByComparingTo("100.00");
    }

    /**
     * A row whose publish never succeeds stops being retried once it reaches the attempt
     * limit (8 in tests): it is marked dead, logged at ERROR, and stays queryable, rather
     * than being retried every 30s forever.
     */
    @Test
    void rowThatKeepsFailingIsMarkedDeadAfterMaxAttemptsAndNotRetriedAgain() {
        Account from = accountRepository.save(new Account(id("SRC"), new BigDecimal("1000.00"), "USD"));
        Account to = accountRepository.save(new Account(id("DST"), new BigDecimal("0.00"), "USD"));
        String paymentId = id("PAY");
        faultInjectingPaymentProducer.failNextPublishes("transactions", paymentId, 1_000);

        post(new PaymentRequest(paymentId, from.getId(), to.getId(), new BigDecimal("100.00"), "USD"));

        // The row only appears once the payment has been processed, so wait on the list.
        OutboxEvent dead = await().atMost(Duration.ofSeconds(40)).until(
                () -> outboxEventRepository.findByMessageKeyAndTopic(paymentId, "transactions"),
                rows -> rows.size() == 1 && rows.get(0).getDeadAt() != null).get(0);
        assertThat(dead.getAttempts()).isEqualTo(8);
        assertThat(dead.getPublishedAt()).isNull();
        assertThat(dead.getLastError()).contains("topic=transactions");
        assertThat(outboxEventRepository.findByDeadAtIsNotNull())
                .extracting(OutboxEvent::getId).contains(dead.getId());

        // Well past the 2s test backoff cap: no further attempts once dead.
        await().during(Duration.ofSeconds(4)).atMost(Duration.ofSeconds(6)).until(() ->
                single(outboxEventRepository.findByMessageKeyAndTopic(paymentId, "transactions")).getAttempts() == 8);
    }

    /**
     * The transaction event is part of the processing transaction: whenever PROCESSED is
     * visible, its outbox row is too - there is no window where one exists without the other.
     */
    @Test
    void processedPaymentAlwaysHasItsTransactionEventInTheOutbox() {
        Account from = accountRepository.save(new Account(id("SRC"), new BigDecimal("1000.00"), "USD"));
        Account to = accountRepository.save(new Account(id("DST"), new BigDecimal("0.00"), "USD"));
        String paymentId = id("PAY");

        post(new PaymentRequest(paymentId, from.getId(), to.getId(), new BigDecimal("100.00"), "USD"));

        await().atMost(Duration.ofSeconds(15)).pollInterval(Duration.ofMillis(20)).until(() ->
                paymentRepository.findById(paymentId).orElseThrow().getStatus() == PaymentStatus.PROCESSED);
        assertThat(outboxEventRepository.findByMessageKeyAndTopic(paymentId, "transactions")).hasSize(1);
    }

    /**
     * Two relays claiming at the same time (two app instances) must never get the same row.
     * The rows here are due an hour from now, so the real relay (which claims with the
     * current time) never touches them; the claims below pass a time past that.
     */
    @Test
    void concurrentClaimsSkipRowsAlreadyLockedByAnotherClaim() throws Exception {
        List<Long> ownRows = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            ownRows.add(jdbcTemplate.queryForObject(
                    "INSERT INTO outbox_events (topic, message_key, payload, next_attempt_at) "
                            + "VALUES ('payments', ?, '{}', now() + interval '1 hour') RETURNING id",
                    Long.class, id("SKIP")));
        }
        Instant claimTime = Instant.now().plus(Duration.ofHours(2));

        try {
            CountDownLatch firstClaimHeld = new CountDownLatch(1);
            CountDownLatch releaseFirstClaim = new CountDownLatch(1);
            CompletableFuture<List<Long>> firstClaim = CompletableFuture.supplyAsync(() ->
                    transactionTemplate.execute(status -> {
                        List<Long> ids = ids(outboxEventRepository.claimPending(claimTime, 2));
                        firstClaimHeld.countDown();
                        awaitLatch(releaseFirstClaim);
                        return ids;
                    }));

            assertThat(firstClaimHeld.await(10, TimeUnit.SECONDS)).isTrue();
            List<Long> secondClaim = transactionTemplate.execute(status ->
                    ids(outboxEventRepository.claimPending(claimTime, 1_000)));
            releaseFirstClaim.countDown();
            List<Long> first = firstClaim.get(10, TimeUnit.SECONDS);

            assertThat(first).hasSize(2);
            assertThat(secondClaim).doesNotContainAnyElementsOf(first);
            List<Long> ownRowsNotInFirst = new ArrayList<>(ownRows);
            ownRowsNotInFirst.removeAll(first);
            assertThat(secondClaim).containsAll(ownRowsNotInFirst);
        } finally {
            for (Long rowId : ownRows) {
                jdbcTemplate.update("DELETE FROM outbox_events WHERE id = ?", rowId);
            }
        }
    }

    private static List<Long> ids(List<OutboxEvent> rows) {
        return rows.stream().map(OutboxEvent::getId).toList();
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            if (!latch.await(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("timed out waiting for latch");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private OutboxEvent awaitPublished(String paymentId, String topic) {
        return await().atMost(Duration.ofSeconds(10)).until(
                () -> single(outboxEventRepository.findByMessageKeyAndTopic(paymentId, topic)),
                row -> row.getPublishedAt() != null);
    }

    private static OutboxEvent single(List<OutboxEvent> rows) {
        assertThat(rows).hasSize(1);
        return rows.get(0);
    }

    private ResponseEntity<PaymentResponse> post(PaymentRequest request) {
        return restTemplate.postForEntity("/payments", request, PaymentResponse.class);
    }

    private static String id(String prefix) {
        return prefix + "-" + UUID.randomUUID();
    }
}
