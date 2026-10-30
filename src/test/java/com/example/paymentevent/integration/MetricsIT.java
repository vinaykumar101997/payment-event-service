package com.example.paymentevent.integration;

import com.example.paymentevent.api.PaymentRequest;
import com.example.paymentevent.domain.Account;
import com.example.paymentevent.domain.Payment;
import com.example.paymentevent.domain.PaymentStatus;
import com.example.paymentevent.metrics.BacklogMetrics;
import com.example.paymentevent.repository.AccountRepository;
import com.example.paymentevent.repository.PaymentRepository;
import com.example.paymentevent.screening.HeldPaymentSweeper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The counters and gauges on /actuator/prometheus move with real payments: final statuses,
 * failed outbox publishes and the backlog they leave, and overdue holds. Other ITs share this
 * context and its registry, so each test compares against the value it read first.
 */
class MetricsIT extends AbstractIntegrationTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private MeterRegistry registry;

    @Autowired
    private BacklogMetrics backlogMetrics;

    @Autowired
    private HeldPaymentSweeper heldPaymentSweeper;

    @Autowired
    private FaultInjectingPaymentProducer faultInjectingPaymentProducer;

    @Autowired
    private AccountRepository accountRepository;

    @Autowired
    private PaymentRepository paymentRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @AfterEach
    void clearInjectedFailures() {
        faultInjectingPaymentProducer.clear();
    }

    @Test
    void processedAndFailedPaymentsAreCountedByStatus() {
        double processedBefore = finalStatus(PaymentStatus.PROCESSED);
        double failedBefore = finalStatus(PaymentStatus.FAILED);
        Account from = accountRepository.save(new Account(id("SRC"), new BigDecimal("100.00"), "USD"));
        Account to = accountRepository.save(new Account(id("DST"), new BigDecimal("0.00"), "USD"));
        String paid = id("PAY");
        String tooMuch = id("PAY");

        assertThat(post(new PaymentRequest(paid, from.getId(), to.getId(), new BigDecimal("60.00"), "USD")))
                .isEqualTo(HttpStatus.CREATED);
        awaitStatus(paid, PaymentStatus.PROCESSED);
        assertThat(post(new PaymentRequest(tooMuch, from.getId(), to.getId(), new BigDecimal("60.00"), "USD")))
                .isEqualTo(HttpStatus.CREATED);
        awaitStatus(tooMuch, PaymentStatus.FAILED);

        // Counted after commit, which may land a moment after the status is visible.
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertThat(finalStatus(PaymentStatus.PROCESSED)).isGreaterThanOrEqualTo(processedBefore + 1);
            assertThat(finalStatus(PaymentStatus.FAILED)).isGreaterThanOrEqualTo(failedBefore + 1);
        });
    }

    @Test
    void failedPublishesAreCountedAndTheRowShowsInTheBacklogUntilItIsSent() {
        double failuresBefore = publishFailures("payments");
        Account from = accountRepository.save(new Account(id("SRC"), new BigDecimal("100.00"), "USD"));
        Account to = accountRepository.save(new Account(id("DST"), new BigDecimal("0.00"), "USD"));
        String paymentId = id("PAY");
        // Backoff 0.5s, 1s, 2s between the failures: the row stays pending for about 3.5s.
        faultInjectingPaymentProducer.failNextPublishes("payments", paymentId, 3);

        assertThat(post(new PaymentRequest(paymentId, from.getId(), to.getId(), new BigDecimal("10.00"), "USD")))
                .isEqualTo(HttpStatus.CREATED);

        await().atMost(Duration.ofSeconds(10)).until(() -> publishFailures("payments") >= failuresBefore + 1);
        backlogMetrics.refresh();
        assertThat(registry.get("payments.outbox.unpublished").gauge().value()).isGreaterThanOrEqualTo(1);
        assertThat(registry.get("payments.outbox.oldest.unpublished.age").gauge().value()).isPositive();

        awaitStatus(paymentId, PaymentStatus.PROCESSED);
        assertThat(publishFailures("payments")).isGreaterThanOrEqualTo(failuresBefore + 3);
    }

    @Test
    void everyHoldTheSweeperMarksOverdueIsCounted() {
        double overdueBefore = registry.get("payments.screening.overdue").counter().count();
        String overdue = heldPayment(Duration.ofMinutes(45));
        // Other ITs may have left old HELD payments unswept; this sweep marks them too.
        Integer toMark = jdbcTemplate.queryForObject("SELECT count(*) FROM payments WHERE status = 'HELD' "
                + "AND overdue_at IS NULL AND created_at < now() - interval '30 minutes'", Integer.class);

        heldPaymentSweeper.sweep();

        assertThat(jdbcTemplate.queryForObject("SELECT overdue_at FROM payments WHERE payment_id = ?",
                Timestamp.class, overdue)).isNotNull();
        assertThat(registry.get("payments.screening.overdue").counter().count()).isEqualTo(overdueBefore + toMark);

        heldPaymentSweeper.sweep();
        assertThat(registry.get("payments.screening.overdue").counter().count()).isEqualTo(overdueBefore + toMark);
    }

    private String heldPayment(Duration age) {
        Account from = accountRepository.save(new Account(id("SRC"), new BigDecimal("100.00"), "USD"));
        Account to = accountRepository.save(new Account(id("DST"), new BigDecimal("0.00"), "USD"));
        String paymentId = id("PAY");
        Payment payment = new Payment(paymentId, from.getId(), to.getId(), new BigDecimal("10.00"), "USD");
        payment.holdForScreening();
        paymentRepository.save(payment);
        jdbcTemplate.update("UPDATE payments SET created_at = ? WHERE payment_id = ?",
                Timestamp.from(Instant.now().minus(age)), paymentId);
        return paymentId;
    }

    private HttpStatus post(PaymentRequest request) {
        return HttpStatus.valueOf(restTemplate.postForEntity("/payments", request, String.class).getStatusCode().value());
    }

    private void awaitStatus(String paymentId, PaymentStatus status) {
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(paymentRepository.findById(paymentId).orElseThrow().getStatus()).isEqualTo(status));
    }

    private double finalStatus(PaymentStatus status) {
        return registry.get("payments.final.status").tag("status", status.name()).counter().count();
    }

    private double publishFailures(String topic) {
        Counter counter = registry.find("payments.outbox.publish.failures").tag("topic", topic).counter();
        return counter == null ? 0 : counter.count();
    }

    private static String id(String prefix) {
        return prefix + "-" + UUID.randomUUID();
    }
}
