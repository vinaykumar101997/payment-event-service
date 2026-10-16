package com.example.paymentevent.integration;

import com.example.paymentevent.domain.Account;
import com.example.paymentevent.domain.Payment;
import com.example.paymentevent.repository.AccountRepository;
import com.example.paymentevent.repository.PaymentRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * T9: GET /payments?status=HELD lists held payments oldest first, with cursor pagination, an
 * optional overdue filter, and the reasons and riskScore of each payment's latest verdict.
 *
 * Every IT shares one database, and other classes leave HELD payments behind. The payments
 * created here are backdated to 1990, so they are the oldest HELD rows and come first; they
 * are deleted after each test.
 */
class HeldPaymentListIT extends AbstractIntegrationTest {

    private static final String TYPE_BASE =
            "https://github.com/vinaykumar101997/payment-event-service/blob/main/docs/problems.md#";
    private static final Instant BASE = Instant.parse("1990-01-01T00:00:00Z");

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

    @LocalServerPort
    private int port;

    private final List<String> created = new ArrayList<>();

    @AfterEach
    void deleteCreatedPayments() {
        for (String paymentId : created) {
            jdbcTemplate.update("DELETE FROM screening_verdicts WHERE payment_id = ?", paymentId);
            jdbcTemplate.update("DELETE FROM payments WHERE payment_id = ?", paymentId);
        }
    }

    @Test
    void heldPaymentsAreListedOldestFirstWithTiesBrokenByPaymentId() throws Exception {
        String third = held("PAY-T9-C", BASE.plusSeconds(20));
        String first = held("PAY-T9-B", BASE.plusSeconds(10));
        String secondSameTime = held("PAY-T9-Z", BASE.plusSeconds(10));
        String rejected = held("PAY-T9-A", BASE.plusSeconds(5));
        jdbcTemplate.update("UPDATE payments SET status = 'REJECTED' WHERE payment_id = ?", rejected);

        JsonNode page = get("/payments?status=HELD&limit=3");

        assertThat(ids(page)).containsExactly(first, secondSameTime, third);
        JsonNode item = page.path("items").get(0);
        assertThat(item.path("amount").decimalValue()).isEqualByComparingTo("12.50");
        assertThat(item.path("currency").asText()).isEqualTo("USD");
        assertThat(item.path("fromAccount").asText()).startsWith("SRC-");
        assertThat(item.path("toAccount").asText()).startsWith("DST-");
        assertThat(Instant.parse(item.path("heldSince").asText())).isEqualTo(BASE.plusSeconds(10));
        assertThat(item.path("overdue").asBoolean()).isFalse();
    }

    @Test
    void cursorPaginationVisitsEveryHeldPaymentExactlyOnce() throws Exception {
        List<String> mine = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            mine.add(held("PAY-T9-P" + i, BASE.plusSeconds(i)));
        }

        JsonNode page1 = get("/payments?status=HELD&limit=2");
        JsonNode page2 = get("/payments?status=HELD&limit=2&cursor=" + page1.path("nextCursor").asText());
        assertThat(ids(page1)).containsExactly(mine.get(0), mine.get(1));
        assertThat(ids(page2)).containsExactly(mine.get(2), mine.get(3));

        List<String> all = new ArrayList<>();
        String cursor = null;
        int pages = 0;
        do {
            JsonNode page = get("/payments?status=HELD&limit=2" + (cursor == null ? "" : "&cursor=" + cursor));
            all.addAll(ids(page));
            cursor = page.path("nextCursor").isNull() ? null : page.path("nextCursor").asText();
            assertThat(++pages).as("pages").isLessThan(1_000);
        } while (cursor != null);

        Set<String> unique = new HashSet<>(all);
        assertThat(unique).hasSameSizeAs(all);
        assertThat(all.subList(0, 5)).containsExactlyElementsOf(mine);
        assertThat(all).hasSize(jdbcTemplate.queryForObject(
                "SELECT count(*) FROM payments WHERE status = 'HELD'", Integer.class));
    }

    @Test
    void overdueFilterSelectsOverdueOrNotOverduePayments() throws Exception {
        String overdue = held("PAY-T9-O1", BASE.plusSeconds(1));
        String fresh = held("PAY-T9-O2", BASE.plusSeconds(2));
        jdbcTemplate.update("UPDATE payments SET overdue_at = now() WHERE payment_id = ?", overdue);

        List<String> onlyOverdue = ids(get("/payments?status=HELD&overdue=true&limit=200"));
        List<String> notOverdue = ids(get("/payments?status=HELD&overdue=false&limit=200"));

        assertThat(onlyOverdue).contains(overdue).doesNotContain(fresh);
        assertThat(notOverdue).contains(fresh).doesNotContain(overdue);
        JsonNode first = get("/payments?status=HELD&overdue=true&limit=1").path("items").get(0);
        assertThat(first.path("paymentId").asText()).isEqualTo(overdue);
        assertThat(first.path("overdue").asBoolean()).isTrue();
    }

    @Test
    void itemsCarryReasonsAndRiskScoreOfTheLatestVerdictOrNull() throws Exception {
        String flagged = held("PAY-T9-V1", BASE.plusSeconds(1));
        String unscreened = held("PAY-T9-V2", BASE.plusSeconds(2));
        verdict(flagged, "ERROR", "[\"source down\"]", null, BASE.plusSeconds(100));
        verdict(flagged, "FLAG", "[\"name match\", \"high-risk corridor\"]", 0.71, BASE.plusSeconds(200));

        JsonNode items = get("/payments?status=HELD&limit=2").path("items");

        assertThat(items.get(0).path("paymentId").asText()).isEqualTo(flagged);
        assertThat(items.get(0).path("reasons")).hasSize(2);
        assertThat(items.get(0).path("reasons").get(0).asText()).isEqualTo("name match");
        assertThat(items.get(0).path("riskScore").asDouble()).isEqualTo(0.71);
        assertThat(items.get(1).path("paymentId").asText()).isEqualTo(unscreened);
        assertThat(items.get(1).path("reasons").isNull()).isTrue();
        assertThat(items.get(1).path("riskScore").isNull()).isTrue();
    }

    @Test
    void invalidQueriesAreRejected() throws Exception {
        assertInvalidQuery("/payments?status=RECEIVED");
        assertInvalidQuery("/payments");
        assertInvalidQuery("/payments?status=HELD&limit=0");
        assertInvalidQuery("/payments?status=HELD&limit=201");
        assertInvalidQuery("/payments?status=HELD&cursor=not-a-cursor");
    }

    @Test
    void listingRequiresTheApiKey() {
        TestRestTemplate bare = new TestRestTemplate(new RestTemplateBuilder().rootUri("http://localhost:" + port));

        ResponseEntity<String> response = bare.getForEntity("/payments?status=HELD", String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
    }

    private void assertInvalidQuery(String path) throws Exception {
        ResponseEntity<String> response = restTemplate.getForEntity(path, String.class);
        assertThat(response.getStatusCode()).as(path).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(objectMapper.readTree(response.getBody()).path("type").asText()).as(path)
                .isEqualTo(TYPE_BASE + "invalid-query");
    }

    private JsonNode get(String path) throws Exception {
        ResponseEntity<String> response = restTemplate.getForEntity(path, String.class);
        assertThat(response.getStatusCode()).as(path).isEqualTo(HttpStatus.OK);
        return objectMapper.readTree(response.getBody());
    }

    private static List<String> ids(JsonNode page) {
        List<String> ids = new ArrayList<>();
        page.path("items").forEach(item -> ids.add(item.path("paymentId").asText()));
        return ids;
    }

    private String held(String prefix, Instant heldSince) {
        Account from = accountRepository.save(new Account("SRC-" + UUID.randomUUID(), new BigDecimal("100.00"), "USD"));
        Account to = accountRepository.save(new Account("DST-" + UUID.randomUUID(), new BigDecimal("0.00"), "USD"));
        String paymentId = prefix + "-" + UUID.randomUUID();
        Payment payment = new Payment(paymentId, from.getId(), to.getId(), new BigDecimal("12.50"), "USD");
        payment.holdForScreening();
        paymentRepository.save(payment);
        jdbcTemplate.update("UPDATE payments SET created_at = ? WHERE payment_id = ?", Timestamp.from(heldSince), paymentId);
        created.add(paymentId);
        return paymentId;
    }

    private void verdict(String paymentId, String verdict, String reasonsJson, Double riskScore, Instant receivedAt) {
        jdbcTemplate.update("INSERT INTO screening_verdicts (payment_id, event_id, verdict, reasons, risk_score,"
                        + " decided_at, screening_run_id, outcome, received_at)"
                        + " VALUES (?, ?, ?, CAST(? AS jsonb), ?, ?, 'run-t9', 'KEPT_HELD', ?)",
                paymentId, UUID.randomUUID(), verdict, reasonsJson, riskScore,
                Timestamp.from(receivedAt), Timestamp.from(receivedAt));
    }
}
