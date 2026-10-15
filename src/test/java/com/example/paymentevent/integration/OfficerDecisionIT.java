package com.example.paymentevent.integration;

import com.example.paymentevent.domain.Account;
import com.example.paymentevent.domain.Payment;
import com.example.paymentevent.domain.PaymentStatus;
import com.example.paymentevent.repository.AccountRepository;
import com.example.paymentevent.repository.PaymentRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
 * S5: POST /payments/{id}/release and /reject. Valid only from HELD, idempotent, recorded with
 * officer, note and time. Release behaves like a CLEAR verdict, reject like a REJECT.
 * HELD payments are written straight to the database, so this runs in the shared
 * screening-off context.
 */
class OfficerDecisionIT extends AbstractIntegrationTest {

    private static final String TYPE_BASE =
            "https://github.com/vinaykumar101997/payment-event-service/blob/main/docs/problems.md#";

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private AccountRepository accountRepository;

    @Autowired
    private PaymentRepository paymentRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    void releaseRecordsTheDecisionAndThePaymentSettles() throws Exception {
        Held held = heldPayment();

        ResponseEntity<String> response = decide(held.paymentId, "release", "alice", "false positive, checked ID");

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(json(response).path("status").asText()).isIn("RECEIVED", "PROCESSING", "PROCESSED");
        Map<String, Object> decision = decisionRow(held.paymentId);
        assertThat(decision).containsEntry("decision", "RELEASE").containsEntry("officer", "alice")
                .containsEntry("note", "false positive, checked ID");
        assertThat(decision.get("decided_at")).isNotNull();
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(paymentRepository.findById(held.paymentId).orElseThrow().getStatus())
                        .isEqualTo(PaymentStatus.PROCESSED));
        assertThat(accountRepository.findById(held.from).orElseThrow().getBalance()).isEqualByComparingTo("75.00");
    }

    @Test
    void repeatingTheSameDecisionIsIdempotent() throws Exception {
        Held held = heldPayment();

        assertThat(decide(held.paymentId, "release", "alice", "ok").getStatusCode()).isEqualTo(HttpStatus.OK);
        ResponseEntity<String> again = decide(held.paymentId, "release", "bob", "ok too");

        assertThat(again.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(decisionRow(held.paymentId)).containsEntry("officer", "alice");
        assertThat(jdbcTemplate.queryForObject("SELECT count(*) FROM officer_decisions WHERE payment_id = ?",
                Integer.class, held.paymentId)).isEqualTo(1);
        assertThat(outboxTopics(held.paymentId)).containsExactly("payments");
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(paymentRepository.findById(held.paymentId).orElseThrow().getStatus())
                        .isEqualTo(PaymentStatus.PROCESSED));
        assertThat(jdbcTemplate.queryForObject("SELECT count(*) FROM ledger_entries WHERE payment_id = ?",
                Integer.class, held.paymentId)).isEqualTo(2);
    }

    @Test
    void rejectRejectsWithTheOfficersNoteAndPublishesPaymentFailed() throws Exception {
        Held held = heldPayment();

        ResponseEntity<String> response = decide(held.paymentId, "reject", "carol", "customer confirmed fraud");
        ResponseEntity<String> again = decide(held.paymentId, "reject", "carol", "customer confirmed fraud");

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(again.getStatusCode()).isEqualTo(HttpStatus.OK);
        Payment payment = paymentRepository.findById(held.paymentId).orElseThrow();
        assertThat(payment.getStatus()).isEqualTo(PaymentStatus.REJECTED);
        assertThat(payment.getFailureReason()).isEqualTo("Rejected by officer carol: customer confirmed fraud");
        assertThat(decisionRow(held.paymentId)).containsEntry("decision", "REJECT").containsEntry("officer", "carol");
        assertThat(outboxTopics(held.paymentId)).containsExactly("payment.failed");
        assertThat(observeRecordKey("payment.failed", held.paymentId, Duration.ofSeconds(20))).isTrue();
        assertThat(accountRepository.findById(held.from).orElseThrow().getBalance()).isEqualByComparingTo("100.00");
    }

    @Test
    void theOppositeDecisionAfterwardsIsAConflict() throws Exception {
        Held rejected = heldPayment();
        decide(rejected.paymentId, "reject", "carol", "fraud");
        Held released = heldPayment();
        decide(released.paymentId, "release", "alice", "fine");

        ResponseEntity<String> releaseRejected = decide(rejected.paymentId, "release", "alice", "changed my mind");
        ResponseEntity<String> rejectReleased = decide(released.paymentId, "reject", "carol", "too late");

        assertNotHeldProblem(releaseRejected, rejected.paymentId, "release");
        assertNotHeldProblem(rejectReleased, released.paymentId, "reject");
        assertThat(paymentRepository.findById(rejected.paymentId).orElseThrow().getStatus())
                .isEqualTo(PaymentStatus.REJECTED);
        assertThat(jdbcTemplate.queryForObject("SELECT count(*) FROM officer_decisions WHERE payment_id IN (?, ?)",
                Integer.class, rejected.paymentId, released.paymentId)).isEqualTo(2);
    }

    @Test
    void aPaymentThatWasNeverHeldCannotBeDecided() throws Exception {
        Held held = heldPayment();
        Payment payment = paymentRepository.findById(held.paymentId).orElseThrow();
        payment.releaseFromHold();
        paymentRepository.save(payment);

        assertNotHeldProblem(decide(held.paymentId, "reject", "carol", "x"), held.paymentId, "reject");
    }

    @Test
    void unknownPaymentIsNotFoundAndOfficerAndNoteAreRequired() throws Exception {
        assertThat(decide("PAY-missing-" + UUID.randomUUID(), "release", "alice", "x").getStatusCode())
                .isEqualTo(HttpStatus.NOT_FOUND);

        Held held = heldPayment();
        ResponseEntity<String> noOfficer = restTemplate.postForEntity("/payments/" + held.paymentId + "/release",
                Map.of("note", "x"), String.class);
        ResponseEntity<String> blankNote = decide(held.paymentId, "release", "alice", " ");

        assertThat(noOfficer.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(json(noOfficer).path("type").asText()).isEqualTo(TYPE_BASE + "validation-failed");
        assertThat(blankNote.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(paymentRepository.findById(held.paymentId).orElseThrow().getStatus()).isEqualTo(PaymentStatus.HELD);
    }

    private void assertNotHeldProblem(ResponseEntity<String> response, String paymentId, String action) throws Exception {
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        JsonNode problem = json(response);
        assertThat(problem.path("type").asText()).isEqualTo(TYPE_BASE + "payment-not-held");
        assertThat(problem.path("instance").asText()).isEqualTo("/payments/" + paymentId + "/" + action);
        assertThat(problem.path("payment").path("paymentId").asText()).isEqualTo(paymentId);
    }

    private ResponseEntity<String> decide(String paymentId, String action, String officer, String note) {
        return restTemplate.postForEntity("/payments/" + paymentId + "/" + action,
                Map.of("officer", officer, "note", note), String.class);
    }

    private Held heldPayment() {
        Account from = accountRepository.save(new Account("SRC-" + UUID.randomUUID(), new BigDecimal("100.00"), "USD"));
        Account to = accountRepository.save(new Account("DST-" + UUID.randomUUID(), new BigDecimal("0.00"), "USD"));
        String paymentId = "PAY-" + UUID.randomUUID();
        Payment payment = new Payment(paymentId, from.getId(), to.getId(), new BigDecimal("25.00"), "USD");
        payment.holdForScreening();
        paymentRepository.save(payment);
        return new Held(paymentId, from.getId());
    }

    private Map<String, Object> decisionRow(String paymentId) {
        return jdbcTemplate.queryForMap(
                "SELECT decision, officer, note, decided_at FROM officer_decisions WHERE payment_id = ?", paymentId);
    }

    private List<Object> outboxTopics(String paymentId) {
        return jdbcTemplate.queryForList("SELECT topic FROM outbox_events WHERE message_key = ?", paymentId)
                .stream().map(row -> row.get("topic")).toList();
    }

    private JsonNode json(ResponseEntity<String> response) throws Exception {
        return objectMapper.readTree(response.getBody());
    }

    private record Held(String paymentId, String from) {
    }
}
