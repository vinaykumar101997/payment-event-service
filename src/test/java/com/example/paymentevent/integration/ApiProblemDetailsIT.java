package com.example.paymentevent.integration;

import com.example.paymentevent.api.PaymentRequest;
import com.example.paymentevent.domain.Account;
import com.example.paymentevent.repository.AccountRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

import java.math.BigDecimal;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Every API error is an RFC 9457 problem: application/problem+json with a stable "type" URI
 * per kind of error (what clients should branch on), a human-readable title and detail, the
 * HTTP status repeated in the body, and the request path as "instance".
 */
class ApiProblemDetailsIT extends AbstractIntegrationTest {

    private static final String TYPE_BASE = "https://example.com/problems/";

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private AccountRepository accountRepository;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    void invalidFieldsAre400ValidationFailedWithPerFieldErrors() throws Exception {
        ResponseEntity<String> response = postJson("{\"paymentId\":\"\",\"fromAccount\":\"A\",\"toAccount\":\"B\","
                + "\"amount\":-5,\"currency\":\"usd\"}");

        JsonNode problem = assertProblem(response, HttpStatus.BAD_REQUEST, "validation-failed", "/payments");
        assertThat(problem.path("errors").has("paymentId")).isTrue();
        assertThat(problem.path("errors").has("amount")).isTrue();
        assertThat(problem.path("errors").path("currency").asText()).contains("ISO currency code");
    }

    @Test
    void unparseableBodyIs400MalformedRequest() throws Exception {
        assertProblem(postJson("{\"paymentId\": "), HttpStatus.BAD_REQUEST, "malformed-request", "/payments");
    }

    @Test
    void sameFromAndToAccountIs400InvalidRequest() throws Exception {
        ResponseEntity<String> response = post(new PaymentRequest(id("PAY"), "ACC-X", "ACC-X", new BigDecimal("1.00"), "USD"));

        JsonNode problem = assertProblem(response, HttpStatus.BAD_REQUEST, "invalid-request", "/payments");
        assertThat(problem.path("detail").asText()).contains("must differ");
    }

    @Test
    void unknownPaymentAndAccountAre404NotFound() throws Exception {
        String paymentId = id("MISSING");
        String accountId = id("MISSING");

        JsonNode payment = assertProblem(restTemplate.getForEntity("/payments/{id}", String.class, paymentId),
                HttpStatus.NOT_FOUND, "not-found", "/payments/" + paymentId);
        assertThat(payment.path("detail").asText()).contains(paymentId);
        assertProblem(restTemplate.getForEntity("/accounts/{id}", String.class, accountId),
                HttpStatus.NOT_FOUND, "not-found", "/accounts/" + accountId);
    }

    /**
     * The idempotent replay keeps its contract - 409, and the existing payment comes back - but
     * as a problem, with the payment in the "payment" extension member.
     */
    @Test
    void identicalRetryIs409DuplicatePaymentCarryingTheExistingPayment() throws Exception {
        Account from = accountRepository.save(new Account(id("SRC"), new BigDecimal("100.00"), "USD"));
        Account to = accountRepository.save(new Account(id("DST"), new BigDecimal("0.00"), "USD"));
        PaymentRequest request = new PaymentRequest(id("PAY"), from.getId(), to.getId(), new BigDecimal("10.00"), "USD");

        assertThat(post(request).getStatusCode()).isEqualTo(HttpStatus.CREATED);
        JsonNode problem = assertProblem(post(request), HttpStatus.CONFLICT, "duplicate-payment", "/payments");

        assertThat(problem.path("payment").path("paymentId").asText()).isEqualTo(request.paymentId());
        assertThat(problem.path("payment").path("fromAccount").asText()).isEqualTo(from.getId());
        assertThat(problem.path("payment").path("amount").decimalValue()).isEqualByComparingTo("10.00");
        assertThat(problem.path("payment").path("status").asText()).isNotEmpty();
    }

    @Test
    void reusedPaymentIdWithDifferentPayloadIs422PaymentIdReused() throws Exception {
        Account from = accountRepository.save(new Account(id("SRC"), new BigDecimal("100.00"), "USD"));
        Account to = accountRepository.save(new Account(id("DST"), new BigDecimal("0.00"), "USD"));
        String paymentId = id("PAY");

        post(new PaymentRequest(paymentId, from.getId(), to.getId(), new BigDecimal("10.00"), "USD"));
        JsonNode problem = assertProblem(
                post(new PaymentRequest(paymentId, from.getId(), to.getId(), new BigDecimal("11.00"), "USD")),
                HttpStatus.UNPROCESSABLE_ENTITY, "payment-id-reused", "/payments");
        assertThat(problem.path("detail").asText()).contains(paymentId);
    }

    @Test
    void unknownAccountIs422UnknownAccount() throws Exception {
        Account to = accountRepository.save(new Account(id("DST"), new BigDecimal("0.00"), "USD"));
        String missing = id("MISSING");

        JsonNode problem = assertProblem(
                post(new PaymentRequest(id("PAY"), missing, to.getId(), new BigDecimal("10.00"), "USD")),
                HttpStatus.UNPROCESSABLE_ENTITY, "unknown-account", "/payments");
        assertThat(problem.path("detail").asText()).contains(missing);
    }

    @Test
    void amountWithMoreDecimalsThanTheCurrencyAllowsIs400InvalidAmount() throws Exception {
        Account from = accountRepository.save(new Account(id("SRC"), new BigDecimal("1000.00"), "JPY"));
        Account to = accountRepository.save(new Account(id("DST"), new BigDecimal("0.00"), "JPY"));
        String paymentId = id("PAY");

        JsonNode problem = assertProblem(
                post(new PaymentRequest(paymentId, from.getId(), to.getId(), new BigDecimal("100.5"), "JPY")),
                HttpStatus.BAD_REQUEST, "invalid-amount", "/payments");
        assertThat(problem.path("detail").asText()).contains("JPY allows (0)");
        assertThat(restTemplate.getForEntity("/payments/{id}", String.class, paymentId).getStatusCode())
                .isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    void wholeYenAmountIsAccepted() {
        Account from = accountRepository.save(new Account(id("SRC"), new BigDecimal("1000.00"), "JPY"));
        Account to = accountRepository.save(new Account(id("DST"), new BigDecimal("0.00"), "JPY"));

        assertThat(post(new PaymentRequest(id("PAY"), from.getId(), to.getId(), new BigDecimal("100"), "JPY"))
                .getStatusCode()).isEqualTo(HttpStatus.CREATED);
    }

    @Test
    void amountAboveTheConfiguredMaximumIs400InvalidAmount() throws Exception {
        Account from = accountRepository.save(new Account(id("SRC"), new BigDecimal("5000000.00"), "USD"));
        Account to = accountRepository.save(new Account(id("DST"), new BigDecimal("0.00"), "USD"));

        JsonNode problem = assertProblem(
                post(new PaymentRequest(id("PAY"), from.getId(), to.getId(), new BigDecimal("1000000.01"), "USD")),
                HttpStatus.BAD_REQUEST, "invalid-amount", "/payments");
        assertThat(problem.path("detail").asText()).contains("exceeds the maximum");
    }

    private JsonNode assertProblem(ResponseEntity<String> response, HttpStatus status, String typeSlug,
                                   String instance) throws Exception {
        assertThat(response.getStatusCode()).isEqualTo(status);
        assertThat(response.getHeaders().getContentType()).isNotNull();
        assertThat(response.getHeaders().getContentType().isCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
                .as("Content-Type %s", response.getHeaders().getContentType()).isTrue();

        JsonNode problem = objectMapper.readTree(response.getBody());
        assertThat(problem.path("type").asText()).isEqualTo(TYPE_BASE + typeSlug);
        assertThat(problem.path("title").asText()).isNotBlank();
        assertThat(problem.path("status").asInt()).isEqualTo(status.value());
        assertThat(problem.path("detail").asText()).isNotBlank();
        assertThat(problem.path("instance").asText()).isEqualTo(instance);
        return problem;
    }

    private ResponseEntity<String> post(PaymentRequest request) {
        return restTemplate.postForEntity("/payments", request, String.class);
    }

    private ResponseEntity<String> postJson(String json) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        return restTemplate.postForEntity("/payments", new HttpEntity<>(json, headers), String.class);
    }

    private static String id(String prefix) {
        return prefix + "-" + UUID.randomUUID();
    }
}
