package com.example.paymentevent.integration;

import com.example.paymentevent.api.PaymentRequest;
import com.example.paymentevent.domain.Account;
import com.example.paymentevent.domain.PaymentStatus;
import com.example.paymentevent.repository.AccountRepository;
import com.example.paymentevent.repository.PaymentRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** S3: verdicts from payment.screening.completed, validated, applied at most once. */
class ScreeningVerdictIT extends AbstractScreeningIntegrationTest {

    private static final String VERDICTS = "payment.screening.completed";

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private AccountRepository accountRepository;

    @Autowired
    private PaymentRepository paymentRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    void clearReleasesTheHeldPaymentWhichThenSettles() throws Exception {
        Held held = createHeldPayment();

        sendRaw(VERDICTS, held.paymentId, verdictJson(uuid(), held.paymentId, "CLEAR"));

        awaitStatus(held.paymentId, PaymentStatus.PROCESSED);
        assertThat(accountRepository.findById(held.from).orElseThrow().getBalance()).isEqualByComparingTo("60.00");
        assertThat(accountRepository.findById(held.to).orElseThrow().getBalance()).isEqualByComparingTo("40.00");
        assertThat(outcomes(held.paymentId)).containsExactly("RELEASED");
    }

    @Test
    void rejectRejectsTheHeldPaymentAndPublishesPaymentFailed() throws Exception {
        Held held = createHeldPayment();

        sendRaw(VERDICTS, held.paymentId, verdictJson(uuid(), held.paymentId, "REJECT", "sanctions hit"));

        awaitStatus(held.paymentId, PaymentStatus.REJECTED);
        assertThat(paymentRepository.findById(held.paymentId).orElseThrow().getFailureReason())
                .isEqualTo("Rejected by screening: sanctions hit");
        assertThat(observeRecordKey("payment.failed", held.paymentId, Duration.ofSeconds(20))).isTrue();
        assertThat(accountRepository.findById(held.from).orElseThrow().getBalance()).isEqualByComparingTo("100.00");
    }

    @Test
    void flagKeepsThePaymentHeld() throws Exception {
        Held held = createHeldPayment();

        sendRaw(VERDICTS, held.paymentId, verdictJson(uuid(), held.paymentId, "FLAG", "name match"));

        await().atMost(Duration.ofSeconds(20)).until(() -> outcomes(held.paymentId).size() == 1);
        assertThat(outcomes(held.paymentId)).containsExactly("KEPT_HELD");
        assertThat(paymentRepository.findById(held.paymentId).orElseThrow().getStatus()).isEqualTo(PaymentStatus.HELD);
    }

    /**
     * The same CLEAR delivered twice, then a REJECT for the same payment: money moves once,
     * the duplicate leaves no trace, the late REJECT is recorded and ignored.
     */
    @Test
    void duplicateAndLateConflictingVerdictsAreHarmless() throws Exception {
        Held held = createHeldPayment();
        String clear = verdictJson(uuid(), held.paymentId, "CLEAR");

        sendRaw(VERDICTS, held.paymentId, clear);
        sendRaw(VERDICTS, held.paymentId, clear);
        awaitStatus(held.paymentId, PaymentStatus.PROCESSED);
        sendRaw(VERDICTS, held.paymentId, verdictJson(uuid(), held.paymentId, "REJECT", "late"));

        await().atMost(Duration.ofSeconds(20)).until(() -> outcomes(held.paymentId).size() == 2);
        assertThat(outcomes(held.paymentId)).containsExactly("RELEASED", "IGNORED_NOT_HELD");
        assertThat(paymentRepository.findById(held.paymentId).orElseThrow().getStatus())
                .isEqualTo(PaymentStatus.PROCESSED);
        assertThat(jdbcTemplate.queryForObject("SELECT count(*) FROM ledger_entries WHERE payment_id = ?",
                Integer.class, held.paymentId)).isEqualTo(2);
        assertThat(accountRepository.findById(held.from).orElseThrow().getBalance()).isEqualByComparingTo("60.00");
    }

    @Test
    void payloadThatFailsTheSchemaGoesToTheDeadLetterTopicUnchanged() throws Exception {
        Held held = createHeldPayment();
        String invalid = verdictJson(uuid(), held.paymentId, "APPROVE");

        sendRaw(VERDICTS, held.paymentId, invalid);

        assertThat(recordValue(VERDICTS + ".DLT", held.paymentId, Duration.ofSeconds(30))).contains(invalid);
        assertThat(outcomes(held.paymentId)).isEmpty();
        assertThat(paymentRepository.findById(held.paymentId).orElseThrow().getStatus()).isEqualTo(PaymentStatus.HELD);
    }

    @Test
    void verdictForAnUnknownPaymentGoesToTheDeadLetterTopic() throws Exception {
        String paymentId = "PAY-unknown-" + uuid();
        String verdict = verdictJson(uuid(), paymentId, "CLEAR");

        sendRaw(VERDICTS, paymentId, verdict);

        assertThat(recordValue(VERDICTS + ".DLT", paymentId, Duration.ofSeconds(30))).contains(verdict);
    }

    private Held createHeldPayment() {
        Account from = accountRepository.save(new Account("SRC-" + uuid(), new BigDecimal("100.00"), "USD"));
        Account to = accountRepository.save(new Account("DST-" + uuid(), new BigDecimal("0.00"), "USD"));
        String paymentId = "PAY-" + uuid();
        assertThat(restTemplate.postForEntity("/payments",
                new PaymentRequest(paymentId, from.getId(), to.getId(), new BigDecimal("40.00"), "USD"), String.class)
                .getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(paymentRepository.findById(paymentId).orElseThrow().getStatus()).isEqualTo(PaymentStatus.HELD);
        return new Held(paymentId, from.getId(), to.getId());
    }

    private void awaitStatus(String paymentId, PaymentStatus status) {
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(paymentRepository.findById(paymentId).orElseThrow().getStatus()).isEqualTo(status));
    }

    private List<String> outcomes(String paymentId) {
        return jdbcTemplate.queryForList(
                "SELECT outcome FROM screening_verdicts WHERE payment_id = ? ORDER BY received_at, event_id",
                String.class, paymentId);
    }

    private static String uuid() {
        return UUID.randomUUID().toString();
    }

    private record Held(String paymentId, String from, String to) {
    }
}
