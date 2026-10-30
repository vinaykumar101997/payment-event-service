package com.example.paymentevent.integration;

import com.example.paymentevent.api.PaymentRequest;
import com.example.paymentevent.repository.PaymentRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

import java.math.BigDecimal;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * S6: every API path needs the X-API-Key header matching PAYMENTS_API_KEY; /actuator/health
 * doesn't. Uses its own client, without the key interceptor every other IT gets from
 * AbstractIntegrationTest.
 */
class ApiKeyAuthIT extends AbstractIntegrationTest {

    private static final String TYPE_BASE =
            "https://github.com/vinaykumar101997/payment-event-service/blob/main/docs/problems.md#";

    @LocalServerPort
    private int port;

    @Autowired
    private PaymentRepository paymentRepository;

    @Autowired
    private ObjectMapper objectMapper;

    private TestRestTemplate client;

    @BeforeEach
    void bareClient() {
        client = new TestRestTemplate(new RestTemplateBuilder().rootUri("http://localhost:" + port));
    }

    @Test
    void requestsWithoutAKeyAreRejectedWith401() throws Exception {
        String paymentId = "PAY-" + UUID.randomUUID();

        assertUnauthorized(call(HttpMethod.GET, "/accounts/ACC-1001", null, null), "/accounts/ACC-1001");
        assertUnauthorized(call(HttpMethod.GET, "/payments/" + paymentId, null, null), "/payments/" + paymentId);
        assertUnauthorized(call(HttpMethod.POST, "/payments",
                new PaymentRequest(paymentId, "ACC-1001", "ACC-1002", new BigDecimal("1.00"), "USD"), null), "/payments");
        assertUnauthorized(call(HttpMethod.POST, "/payments/" + paymentId + "/release",
                Map.of("officer", "alice", "note", "x"), null), "/payments/" + paymentId + "/release");
        assertUnauthorized(call(HttpMethod.POST, "/payments/" + paymentId + "/reject",
                Map.of("officer", "alice", "note", "x"), null), "/payments/" + paymentId + "/reject");
        assertThat(paymentRepository.findById(paymentId)).isEmpty();
    }

    @Test
    void requestsWithAWrongKeyAreRejectedWith401() throws Exception {
        String paymentId = "PAY-" + UUID.randomUUID();

        assertUnauthorized(call(HttpMethod.GET, "/accounts/ACC-1001", null, "wrong-key"), "/accounts/ACC-1001");
        assertUnauthorized(call(HttpMethod.GET, "/accounts/ACC-1001", null, TEST_API_KEY + "x"), "/accounts/ACC-1001");
        assertUnauthorized(call(HttpMethod.GET, "/accounts/ACC-1001", null, TEST_API_KEY.toUpperCase()),
                "/accounts/ACC-1001");
        assertUnauthorized(call(HttpMethod.GET, "/accounts/ACC-1001", null, ""), "/accounts/ACC-1001");
        assertUnauthorized(call(HttpMethod.POST, "/payments",
                new PaymentRequest(paymentId, "ACC-1001", "ACC-1002", new BigDecimal("1.00"), "USD"), "wrong-key"),
                "/payments");
        assertThat(paymentRepository.findById(paymentId)).isEmpty();
    }

    @Test
    void theRightKeyIsAccepted() {
        ResponseEntity<String> response = call(HttpMethod.GET, "/accounts/ACC-1001", null, TEST_API_KEY);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    /** Health and info answer without a key, so liveness checks need no secret; health shows no details. */
    @Test
    void healthAndInfoAreNotBehindTheKey() {
        ResponseEntity<String> health = call(HttpMethod.GET, "/actuator/health", null, null);
        ResponseEntity<String> info = call(HttpMethod.GET, "/actuator/info", null, null);

        assertThat(health.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(health.getBody()).contains("\"status\":\"UP\"").doesNotContain("components");
        assertThat(info.getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    void prometheusNeedsTheKey() throws Exception {
        assertUnauthorized(call(HttpMethod.GET, "/actuator/prometheus", null, null), "/actuator/prometheus");
        assertUnauthorized(call(HttpMethod.GET, "/actuator/prometheus", null, "wrong-key"), "/actuator/prometheus");

        ResponseEntity<String> scrape = call(HttpMethod.GET, "/actuator/prometheus", null, TEST_API_KEY);

        assertThat(scrape.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(scrape.getBody()).contains(
                "payments_final_status_total{status=\"PROCESSED\"}",
                "payments_screening_overdue_total",
                "payments_outbox_unpublished",
                "payments_outbox_oldest_unpublished_age_seconds",
                "payments_screening_held");
        assertThat(scrape.getBody()).doesNotContain(TEST_API_KEY);
    }

    private void assertUnauthorized(ResponseEntity<String> response, String path) throws Exception {
        assertThat(response.getStatusCode()).as(path).isEqualTo(HttpStatus.UNAUTHORIZED);
        assertThat(response.getHeaders().getContentType()).isNotNull();
        assertThat(response.getHeaders().getContentType().isCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON)).isTrue();
        JsonNode problem = objectMapper.readTree(response.getBody());
        assertThat(problem.path("type").asText()).isEqualTo(TYPE_BASE + "unauthorized");
        assertThat(problem.path("status").asInt()).isEqualTo(401);
        assertThat(problem.path("instance").asText()).isEqualTo(path);
        assertThat(response.getBody()).doesNotContain(TEST_API_KEY);
    }

    private ResponseEntity<String> call(HttpMethod method, String path, Object body, String apiKey) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        if (apiKey != null) {
            headers.set("X-API-Key", apiKey);
        }
        return client.exchange(path, method, new HttpEntity<>(body, headers), String.class);
    }
}
