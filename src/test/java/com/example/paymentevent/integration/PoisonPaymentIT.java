package com.example.paymentevent.integration;

import com.example.paymentevent.api.PaymentRequest;
import com.example.paymentevent.api.PaymentResponse;
import com.example.paymentevent.domain.Account;
import com.example.paymentevent.domain.Payment;
import com.example.paymentevent.domain.PaymentStatus;
import com.example.paymentevent.repository.AccountRepository;
import com.example.paymentevent.repository.LedgerEntryRepository;
import com.example.paymentevent.repository.OutboxEventRepository;
import com.example.paymentevent.repository.PaymentRepository;
import com.example.paymentevent.service.ReconciliationService;
import com.example.paymentevent.sweep.PaymentSweeper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.jdbc.core.JdbcTemplate;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * A "poison" payment: one whose processing fails every time with a non-business error.
 * A Postgres trigger makes the ledger insert raise for payment ids starting with POISON-,
 * so process() fails deterministically for this payment only, with a real SQL error, after
 * already having debited/credited in the same (rolled-back) transaction. The trigger is
 * dropped after each test; it never matches the ids any other test uses.
 */
class PoisonPaymentIT extends AbstractIntegrationTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private AccountRepository accountRepository;

    @Autowired
    private PaymentRepository paymentRepository;

    @Autowired
    private LedgerEntryRepository ledgerEntryRepository;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private ReconciliationService reconciliationService;

    @Autowired
    private PaymentSweeper paymentSweeper;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeEach
    void installPoisonTrigger() {
        jdbcTemplate.execute("CREATE OR REPLACE FUNCTION test_poison_ledger() RETURNS trigger AS $$ "
                + "BEGIN IF NEW.payment_id LIKE 'POISON-%' THEN "
                + "RAISE EXCEPTION 'injected poison for %', NEW.payment_id; END IF; RETURN NEW; END $$ "
                + "LANGUAGE plpgsql");
        jdbcTemplate.execute("DROP TRIGGER IF EXISTS test_poison_ledger ON ledger_entries");
        jdbcTemplate.execute("CREATE TRIGGER test_poison_ledger BEFORE INSERT ON ledger_entries "
                + "FOR EACH ROW EXECUTE FUNCTION test_poison_ledger()");
    }

    @AfterEach
    void dropPoisonTrigger() {
        jdbcTemplate.execute("DROP TRIGGER IF EXISTS test_poison_ledger ON ledger_entries");
        jdbcTemplate.execute("DROP FUNCTION IF EXISTS test_poison_ledger()");
    }

    @Test
    void paymentThatKeepsFailingIsPoisonedAfterMaxAttemptsInsteadOfRetriedForever() {
        Account from = accountRepository.save(new Account(id("SRC"), new BigDecimal("1000.00"), "USD"));
        Account to = accountRepository.save(new Account(id("DST"), new BigDecimal("0.00"), "USD"));
        String paymentId = id("POISON");

        restTemplate.postForEntity("/payments",
                new PaymentRequest(paymentId, from.getId(), to.getId(), new BigDecimal("100.00"), "USD"),
                PaymentResponse.class);

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(paymentRepository.findById(paymentId).orElseThrow().getStatus())
                        .isEqualTo(PaymentStatus.POISONED));

        Payment poisoned = paymentRepository.findById(paymentId).orElseThrow();
        assertThat(poisoned.getProcessingAttempts()).isEqualTo(3);
        assertThat(poisoned.getFailureReason()).contains("3 failed processing attempts").contains("injected poison");
        assertThat(ledgerEntryRepository.findByPaymentId(paymentId)).isEmpty();
        assertThat(accountRepository.findById(from.getId()).orElseThrow().getBalance()).isEqualByComparingTo("1000.00");
        assertThat(accountRepository.findById(to.getId()).orElseThrow().getBalance()).isEqualByComparingTo("0.00");
        assertThat(reconciliationService.reconcile().isClean()).isTrue();
        // The poison message itself is parked on the DLT.
        assertThat(observeRecordKey("payments.DLT", paymentId, Duration.ofSeconds(15))).isTrue();

        // And nothing republishes it: even once it's old enough for the sweeper, POISONED is
        // terminal, so the sweeper leaves it alone and no new "payments" event is enqueued.
        jdbcTemplate.update("UPDATE payments SET created_at = now() - interval '5 minutes' WHERE payment_id = ?",
                paymentId);
        paymentSweeper.sweep();
        assertThat(outboxEventRepository.findByMessageKeyAndTopic(paymentId, "payments")).hasSize(1);
        assertThat(paymentRepository.findById(paymentId).orElseThrow().getStatus()).isEqualTo(PaymentStatus.POISONED);
    }

    /** Business failures are final outcomes, not faults: they never count toward poisoning. */
    @Test
    void businessFailureDoesNotCountAsAProcessingAttempt() {
        Account from = accountRepository.save(new Account(id("SRC"), new BigDecimal("10.00"), "USD"));
        Account to = accountRepository.save(new Account(id("DST"), new BigDecimal("0.00"), "USD"));
        String paymentId = id("PAY");

        restTemplate.postForEntity("/payments",
                new PaymentRequest(paymentId, from.getId(), to.getId(), new BigDecimal("100.00"), "USD"),
                PaymentResponse.class);

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(paymentRepository.findById(paymentId).orElseThrow().getStatus())
                        .isEqualTo(PaymentStatus.FAILED));
        assertThat(paymentRepository.findById(paymentId).orElseThrow().getProcessingAttempts()).isZero();
    }

    private static String id(String prefix) {
        return prefix + "-" + UUID.randomUUID();
    }
}
