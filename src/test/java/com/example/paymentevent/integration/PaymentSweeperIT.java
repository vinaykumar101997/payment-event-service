package com.example.paymentevent.integration;

import com.example.paymentevent.domain.Account;
import com.example.paymentevent.domain.Payment;
import com.example.paymentevent.domain.PaymentStatus;
import com.example.paymentevent.repository.AccountRepository;
import com.example.paymentevent.repository.OutboxEventRepository;
import com.example.paymentevent.repository.PaymentRepository;
import com.example.paymentevent.sweep.PaymentSweeper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.awaitility.Awaitility.await;

class PaymentSweeperIT extends AbstractIntegrationTest {

    @Autowired
    private PaymentSweeper paymentSweeper;

    @Autowired
    private AccountRepository accountRepository;

    @Autowired
    private PaymentRepository paymentRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private FaultInjectingPaymentProducer faultInjectingPaymentProducer;

    @AfterEach
    void cleanUp() {
        faultInjectingPaymentProducer.clear();
        jdbcTemplate.execute("DROP TRIGGER IF EXISTS test_fail_outbox_insert ON outbox_events");
        jdbcTemplate.execute("DROP FUNCTION IF EXISTS test_fail_outbox_insert()");
    }

    /**
     * Simulates a payment left RECEIVED with no event that will ever be processed (e.g. its
     * message went to the DLT after retryable failures): the row is inserted directly, with
     * no outbox row. Backdating created_at past the sweeper's 60s threshold and calling
     * sweep() directly (rather than waiting on the scheduler) exercises markSwept() outside
     * of any caller-provided transaction, then checks the republished event is processed
     * exactly once.
     */
    @Test
    void sweepRepublishesStaleReceivedPaymentAndItIsProcessedOnce() {
        Account from = accountRepository.save(new Account(id("SRC"), new BigDecimal("1000.00"), "USD"));
        Account to = accountRepository.save(new Account(id("DST"), new BigDecimal("0.00"), "USD"));
        String paymentId = id("PAY");
        paymentRepository.saveAndFlush(new Payment(paymentId, from.getId(), to.getId(), new BigDecimal("100.00"), "USD"));
        jdbcTemplate.update("UPDATE payments SET created_at = now() - interval '5 minutes' WHERE payment_id = ?",
                paymentId);

        assertThatCode(() -> paymentSweeper.sweep()).doesNotThrowAnyException();

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(paymentRepository.findById(paymentId).orElseThrow().getStatus())
                        .isEqualTo(PaymentStatus.PROCESSED));

        // A second sweep inside the cooldown window must not pick the payment up again.
        assertThatCode(() -> paymentSweeper.sweep()).doesNotThrowAnyException();

        Payment swept = paymentRepository.findById(paymentId).orElseThrow();
        assertThat(swept.getLastSweptAt()).isNotNull();
        // Single publish path: the sweeper enqueued to the outbox rather than publishing itself.
        assertThat(outboxEventRepository.findByMessageKeyAndTopic(paymentId, "payments")).hasSize(1);
        assertThat(accountRepository.findById(from.getId()).orElseThrow().getBalance())
                .isEqualByComparingTo("900.00");
        assertThat(accountRepository.findById(to.getId()).orElseThrow().getBalance())
                .isEqualByComparingTo("100.00");
    }

    /**
     * One stale payment whose re-enqueue fails must not stop the rest of the batch. The failure
     * is injected at both places a sweep could hand a payment off - a Kafka publish (the old
     * direct path) and the outbox insert (via a trigger on its message_key) - so the test
     * targets the behaviour, not one implementation. The failing payment is the older one, so
     * it's attempted first.
     */
    @Test
    void oneFailingPaymentDoesNotAbortTheRestOfTheSweep() {
        Account from = accountRepository.save(new Account(id("SRC"), new BigDecimal("1000.00"), "USD"));
        Account to = accountRepository.save(new Account(id("DST"), new BigDecimal("0.00"), "USD"));
        String failing = insertStalePayment(id("SWEEPFAIL"), from, to, "10 minutes");
        String healthy = insertStalePayment(id("PAY"), from, to, "5 minutes");

        faultInjectingPaymentProducer.failNextPublishes("payments", failing, 1_000);
        jdbcTemplate.execute("CREATE OR REPLACE FUNCTION test_fail_outbox_insert() RETURNS trigger AS $$ "
                + "BEGIN IF NEW.message_key LIKE 'SWEEPFAIL-%' THEN "
                + "RAISE EXCEPTION 'injected outbox failure for %', NEW.message_key; END IF; RETURN NEW; END $$ "
                + "LANGUAGE plpgsql");
        jdbcTemplate.execute("CREATE TRIGGER test_fail_outbox_insert BEFORE INSERT ON outbox_events "
                + "FOR EACH ROW EXECUTE FUNCTION test_fail_outbox_insert()");

        assertThatCode(() -> paymentSweeper.sweep()).doesNotThrowAnyException();

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(paymentRepository.findById(healthy).orElseThrow().getStatus())
                        .isEqualTo(PaymentStatus.PROCESSED));
        assertThat(paymentRepository.findById(failing).orElseThrow().getStatus()).isEqualTo(PaymentStatus.RECEIVED);
    }

    /**
     * Multiple app instances sweep concurrently. Each stale payment must be handed off exactly
     * once, not once per instance: the claim (UPDATE ... FOR UPDATE SKIP LOCKED) gives every
     * payment to exactly one sweep, and the cooldown keeps the others from re-claiming it.
     */
    @Test
    void concurrentSweepsHandOffEachStalePaymentExactlyOnce() throws Exception {
        Account from = accountRepository.save(new Account(id("SRC"), new BigDecimal("1000.00"), "USD"));
        Account to = accountRepository.save(new Account(id("DST"), new BigDecimal("0.00"), "USD"));
        List<String> stale = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            stale.add(insertStalePayment(id("PAY"), from, to, "5 minutes"));
        }

        CountDownLatch start = new CountDownLatch(1);
        List<CompletableFuture<Void>> sweeps = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            sweeps.add(CompletableFuture.runAsync(() -> {
                try {
                    start.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                paymentSweeper.sweep();
            }));
        }
        start.countDown();
        CompletableFuture.allOf(sweeps.toArray(CompletableFuture[]::new)).get(30, TimeUnit.SECONDS);

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                assertThat(paymentRepository.findAllById(stale))
                        .allMatch(p -> p.getStatus() == PaymentStatus.PROCESSED));
        for (String paymentId : stale) {
            assertThat(faultInjectingPaymentProducer.publishCount("payments", paymentId))
                    .as("publishes of %s", paymentId).isEqualTo(1);
            assertThat(outboxEventRepository.findByMessageKeyAndTopic(paymentId, "payments"))
                    .as("outbox rows for %s", paymentId).hasSize(1);
        }
        assertThat(accountRepository.findById(from.getId()).orElseThrow().getBalance())
                .isEqualByComparingTo("800.00");
    }

    private String insertStalePayment(String paymentId, Account from, Account to, String age) {
        paymentRepository.saveAndFlush(new Payment(paymentId, from.getId(), to.getId(), new BigDecimal("10.00"), "USD"));
        jdbcTemplate.update("UPDATE payments SET created_at = now() - interval '" + age + "' WHERE payment_id = ?",
                paymentId);
        return paymentId;
    }

    private static String id(String prefix) {
        return prefix + "-" + UUID.randomUUID();
    }
}
