package com.example.paymentevent.integration;

import com.example.paymentevent.domain.Account;
import com.example.paymentevent.domain.Payment;
import com.example.paymentevent.domain.PaymentStatus;
import com.example.paymentevent.repository.AccountRepository;
import com.example.paymentevent.repository.PaymentRepository;
import com.example.paymentevent.screening.HeldPaymentSweeper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * S4: never auto-approve. A payment HELD longer than payments.screening.timeout (PT30M in the
 * ITs) is marked overdue, logged and reported to the hook once - and stays HELD, with no
 * outbox event and no money moved.
 */
class HeldPaymentSweeperIT extends AbstractIntegrationTest {

    @Autowired
    private HeldPaymentSweeper sweeper;

    @Autowired
    private RecordingOverdueHook hook;

    @Autowired
    private AccountRepository accountRepository;

    @Autowired
    private PaymentRepository paymentRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    void overdueHeldPaymentIsMarkedAndReportedButStaysHeld() {
        String overdue = heldPayment(Duration.ofMinutes(31));
        String recent = heldPayment(Duration.ofMinutes(5));

        sweeper.sweep();

        assertThat(paymentRepository.findById(overdue).orElseThrow().getStatus()).isEqualTo(PaymentStatus.HELD);
        assertThat(overdueAt(overdue)).isNotNull();
        assertThat(hook.callsFor(overdue)).isEqualTo(1);
        assertThat(outboxTopics(overdue)).isEmpty();

        assertThat(overdueAt(recent)).isNull();
        assertThat(hook.callsFor(recent)).isZero();
    }

    @Test
    void anOverduePaymentIsReportedOnlyOnce() {
        String overdue = heldPayment(Duration.ofHours(3));

        sweeper.sweep();
        Timestamp firstMark = overdueAt(overdue);
        sweeper.sweep();

        assertThat(hook.callsFor(overdue)).isEqualTo(1);
        assertThat(overdueAt(overdue)).isEqualTo(firstMark);
        assertThat(paymentRepository.findById(overdue).orElseThrow().getStatus()).isEqualTo(PaymentStatus.HELD);
    }

    /** Only HELD payments are candidates: an old RECEIVED payment is PaymentSweeper's business. */
    @Test
    void paymentsThatAreNotHeldAreNeverMarked() {
        Account from = accountRepository.save(new Account(id("SRC"), new BigDecimal("100.00"), "USD"));
        Account to = accountRepository.save(new Account(id("DST"), new BigDecimal("0.00"), "USD"));
        String rejected = id("PAY");
        Payment payment = new Payment(rejected, from.getId(), to.getId(), new BigDecimal("10.00"), "USD");
        payment.holdForScreening();
        payment.reject("test");
        paymentRepository.save(payment);
        backdate(rejected, Duration.ofHours(2));

        sweeper.sweep();

        assertThat(overdueAt(rejected)).isNull();
        assertThat(hook.callsFor(rejected)).isZero();
    }

    private String heldPayment(Duration age) {
        Account from = accountRepository.save(new Account(id("SRC"), new BigDecimal("100.00"), "USD"));
        Account to = accountRepository.save(new Account(id("DST"), new BigDecimal("0.00"), "USD"));
        String paymentId = id("PAY");
        Payment payment = new Payment(paymentId, from.getId(), to.getId(), new BigDecimal("10.00"), "USD");
        payment.holdForScreening();
        paymentRepository.save(payment);
        backdate(paymentId, age);
        return paymentId;
    }

    private void backdate(String paymentId, Duration age) {
        jdbcTemplate.update("UPDATE payments SET created_at = ? WHERE payment_id = ?",
                Timestamp.from(Instant.now().minus(age)), paymentId);
    }

    private Timestamp overdueAt(String paymentId) {
        return jdbcTemplate.queryForObject("SELECT overdue_at FROM payments WHERE payment_id = ?",
                Timestamp.class, paymentId);
    }

    private List<Object> outboxTopics(String paymentId) {
        return jdbcTemplate.queryForList("SELECT topic FROM outbox_events WHERE message_key = ?", paymentId)
                .stream().map((Map<String, Object> row) -> row.get("topic")).toList();
    }

    private static String id(String prefix) {
        return prefix + "-" + UUID.randomUUID();
    }
}
