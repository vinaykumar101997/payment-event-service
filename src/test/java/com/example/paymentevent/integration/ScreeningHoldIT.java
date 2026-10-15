package com.example.paymentevent.integration;

import com.example.paymentevent.api.PaymentRequest;
import com.example.paymentevent.domain.Account;
import com.example.paymentevent.domain.PaymentStatus;
import com.example.paymentevent.repository.AccountRepository;
import com.example.paymentevent.repository.PaymentRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.Schema;
import com.networknt.schema.SchemaRegistry;
import com.networknt.schema.SchemaRegistryConfig;
import com.networknt.schema.SpecificationVersion;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.InputStream;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** S2: with screening on, a new payment is HELD and only a screening request leaves the service. */
class ScreeningHoldIT extends AbstractScreeningIntegrationTest {

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
    void createdPaymentIsHeldAndOnlyAScreeningRequestIsPublished() throws Exception {
        Account from = accountRepository.save(new Account(id("SRC"), new BigDecimal("100.00"), "USD"));
        Account to = accountRepository.save(new Account(id("DST"), new BigDecimal("0.00"), "USD"));
        String paymentId = id("PAY");

        ResponseEntity<String> response = restTemplate.postForEntity("/payments",
                new PaymentRequest(paymentId, from.getId(), to.getId(), new BigDecimal("25.00"), "USD"), String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(objectMapper.readTree(response.getBody()).path("status").asText()).isEqualTo("HELD");

        List<Map<String, Object>> outbox = jdbcTemplate.queryForList(
                "SELECT topic, payload FROM outbox_events WHERE message_key = ?", paymentId);
        assertThat(outbox).extracting(row -> row.get("topic")).containsExactly("payment.screening.requested");

        JsonNode payload = objectMapper.readTree((String) outbox.get(0).get("payload"));
        assertThat(requestedSchema().validate(payload)).isEmpty();
        assertThat(payload.path("paymentId").asText()).isEqualTo(paymentId);
        assertThat(payload.path("amount").asText()).isEqualTo("25.00");

        assertThat(observeRecordKey("payment.screening.requested", paymentId, Duration.ofSeconds(20))).isTrue();
        assertThat(observeRecordKey("payments", paymentId, Duration.ofSeconds(3))).isFalse();
        await().during(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(paymentRepository.findById(paymentId).orElseThrow().getStatus())
                        .isEqualTo(PaymentStatus.HELD));
        assertThat(accountRepository.findById(from.getId()).orElseThrow().getBalance()).isEqualByComparingTo("100.00");
    }

    private static Schema requestedSchema() throws Exception {
        SchemaRegistry registry = SchemaRegistry.withDefaultDialect(SpecificationVersion.DRAFT_2020_12,
                builder -> builder.schemaRegistryConfig(SchemaRegistryConfig.builder().formatAssertionsEnabled(true).build()));
        try (InputStream in = ScreeningHoldIT.class.getResourceAsStream(
                "/contracts/payment.screening.requested.v1.schema.json")) {
            return registry.getSchema(in);
        }
    }

    private static String id(String prefix) {
        return prefix + "-" + UUID.randomUUID();
    }
}
