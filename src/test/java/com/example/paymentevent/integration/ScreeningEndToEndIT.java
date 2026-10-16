package com.example.paymentevent.integration;

import com.example.paymentevent.api.PaymentRequest;
import com.example.paymentevent.domain.Account;
import com.example.paymentevent.domain.PaymentStatus;
import com.example.paymentevent.repository.AccountRepository;
import com.example.paymentevent.repository.PaymentRepository;
import com.example.paymentevent.service.ReconciliationService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * S7: the screening hold end to end, over real Kafka and Postgres, the way the Python
 * screening service would drive it: our payment.screening.requested goes out, verdicts come
 * back as plain JSON on payment.screening.completed, officers use the REST endpoints. After
 * every scenario the whole ledger must still reconcile. The screening-off flow is
 * ScreeningDisabledEndToEndIT.
 */
class ScreeningEndToEndIT extends AbstractScreeningIntegrationTest {

    private static final String VERDICTS = "payment.screening.completed";
    private static final Duration WAIT = Duration.ofSeconds(30);

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private AccountRepository accountRepository;

    @Autowired
    private PaymentRepository paymentRepository;

    @Autowired
    private ReconciliationService reconciliationService;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @AfterEach
    void ledgerStillReconciles() {
        assertThat(reconciliationService.reconcile().isClean()).isTrue();
    }

    @Test
    void cleanPaymentSettles() throws Exception {
        Flow flow = createHeldPayment("30.00");
        assertThat(observeRecordKey("payment.screening.requested", flow.paymentId, WAIT)).isTrue();

        sendRaw(VERDICTS, flow.paymentId, verdictJson(uuid(), flow.paymentId, "CLEAR"));

        awaitStatus(flow.paymentId, PaymentStatus.PROCESSED);
        assertBalances(flow, "70.00", "30.00");
        assertThat(observeRecordKey("transactions", flow.paymentId, WAIT)).isTrue();
    }

    @Test
    void flaggedPaymentIsHeldThenReleasedByAnOfficerAndSettles() throws Exception {
        Flow flow = createHeldPayment("30.00");

        sendRaw(VERDICTS, flow.paymentId, verdictJson(uuid(), flow.paymentId, "FLAG", "name close to watch list"));
        await().atMost(WAIT).until(() -> verdictOutcomes(flow.paymentId).equals(List.of("KEPT_HELD")));
        assertThat(status(flow.paymentId)).isEqualTo(PaymentStatus.HELD);

        ResponseEntity<String> release = officer(flow.paymentId, "release", "alice", "different person, checked DOB");

        assertThat(release.getStatusCode()).isEqualTo(HttpStatus.OK);
        awaitStatus(flow.paymentId, PaymentStatus.PROCESSED);
        assertBalances(flow, "70.00", "30.00");
    }

    @Test
    void flaggedPaymentIsRejectedByAnOfficer() throws Exception {
        Flow flow = createHeldPayment("30.00");

        sendRaw(VERDICTS, flow.paymentId, verdictJson(uuid(), flow.paymentId, "FLAG", "high-risk corridor"));
        await().atMost(WAIT).until(() -> verdictOutcomes(flow.paymentId).equals(List.of("KEPT_HELD")));

        ResponseEntity<String> reject = officer(flow.paymentId, "reject", "carol", "customer confirmed fraud");

        assertThat(reject.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(status(flow.paymentId)).isEqualTo(PaymentStatus.REJECTED);
        assertThat(observeRecordKey("payment.failed", flow.paymentId, WAIT)).isTrue();
        assertBalances(flow, "100.00", "0.00");
        assertThat(ledgerEntries(flow.paymentId)).isZero();
    }

    @Test
    void duplicateVerdictIsHarmless() throws Exception {
        Flow flow = createHeldPayment("30.00");
        String clear = verdictJson(uuid(), flow.paymentId, "CLEAR");

        sendRaw(VERDICTS, flow.paymentId, clear);
        sendRaw(VERDICTS, flow.paymentId, clear);

        awaitStatus(flow.paymentId, PaymentStatus.PROCESSED);
        // Give the second delivery time to be consumed before checking it changed nothing.
        await().pollDelay(Duration.ofSeconds(3)).atMost(WAIT).untilAsserted(() -> {
            assertThat(verdictOutcomes(flow.paymentId)).containsExactly("RELEASED");
            assertThat(ledgerEntries(flow.paymentId)).isEqualTo(2);
            assertThat(outboxCount(flow.paymentId, "payments")).isEqualTo(1);
        });
        assertBalances(flow, "70.00", "30.00");
    }

    @Test
    void errorVerdictKeepsThePaymentHeld() throws Exception {
        Flow flow = createHeldPayment("30.00");

        sendRaw(VERDICTS, flow.paymentId, verdictJson(uuid(), flow.paymentId, "ERROR", "watch-list source unavailable"));

        await().atMost(WAIT).until(() -> verdictOutcomes(flow.paymentId).equals(List.of("KEPT_HELD")));
        await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(status(flow.paymentId)).isEqualTo(PaymentStatus.HELD));
        assertThat(outboxCount(flow.paymentId, "payments")).isZero();
        assertBalances(flow, "100.00", "0.00");
    }

    @Test
    void payloadThatFailsTheSchemaGoesToTheDeadLetterTopic() throws Exception {
        Flow flow = createHeldPayment("30.00");
        String missingVerdict = verdictJson(uuid(), flow.paymentId, "CLEAR").replace("\"verdict\":\"CLEAR\",", "");

        sendRaw(VERDICTS, flow.paymentId, missingVerdict);

        assertThat(recordValue(VERDICTS + ".DLT", flow.paymentId, WAIT)).contains(missingVerdict);
        assertThat(verdictOutcomes(flow.paymentId)).isEmpty();
        assertThat(status(flow.paymentId)).isEqualTo(PaymentStatus.HELD);
        assertBalances(flow, "100.00", "0.00");
    }

    private Flow createHeldPayment(String amount) {
        Account from = accountRepository.save(new Account("SRC-" + uuid(), new BigDecimal("100.00"), "USD"));
        Account to = accountRepository.save(new Account("DST-" + uuid(), new BigDecimal("0.00"), "USD"));
        String paymentId = "PAY-" + uuid();
        ResponseEntity<String> created = restTemplate.postForEntity("/payments",
                new PaymentRequest(paymentId, from.getId(), to.getId(), new BigDecimal(amount), "USD"), String.class);
        assertThat(created.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(status(paymentId)).isEqualTo(PaymentStatus.HELD);
        return new Flow(paymentId, from.getId(), to.getId());
    }

    private ResponseEntity<String> officer(String paymentId, String action, String officer, String note) {
        return restTemplate.postForEntity("/payments/" + paymentId + "/" + action,
                Map.of("officer", officer, "note", note), String.class);
    }

    private PaymentStatus status(String paymentId) {
        return paymentRepository.findById(paymentId).orElseThrow().getStatus();
    }

    private void awaitStatus(String paymentId, PaymentStatus expected) {
        await().atMost(WAIT).untilAsserted(() -> assertThat(status(paymentId)).isEqualTo(expected));
    }

    private void assertBalances(Flow flow, String fromBalance, String toBalance) {
        assertThat(accountRepository.findById(flow.from).orElseThrow().getBalance()).isEqualByComparingTo(fromBalance);
        assertThat(accountRepository.findById(flow.to).orElseThrow().getBalance()).isEqualByComparingTo(toBalance);
    }

    private List<String> verdictOutcomes(String paymentId) {
        return jdbcTemplate.queryForList("SELECT outcome FROM screening_verdicts WHERE payment_id = ? ORDER BY received_at",
                String.class, paymentId);
    }

    private int ledgerEntries(String paymentId) {
        return jdbcTemplate.queryForObject("SELECT count(*) FROM ledger_entries WHERE payment_id = ?",
                Integer.class, paymentId);
    }

    private int outboxCount(String paymentId, String topic) {
        return jdbcTemplate.queryForObject("SELECT count(*) FROM outbox_events WHERE message_key = ? AND topic = ?",
                Integer.class, paymentId, topic);
    }

    private static String uuid() {
        return UUID.randomUUID().toString();
    }

    private record Flow(String paymentId, String from, String to) {
    }
}
