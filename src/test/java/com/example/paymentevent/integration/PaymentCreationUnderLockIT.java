package com.example.paymentevent.integration;

import com.example.paymentevent.api.PaymentRequest;
import com.example.paymentevent.domain.Account;
import com.example.paymentevent.domain.PaymentStatus;
import com.example.paymentevent.repository.AccountRepository;
import com.example.paymentevent.repository.PaymentRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Inserting a payment runs a foreign-key check that takes FOR KEY SHARE on both referenced
 * account rows. FOR KEY SHARE conflicts only with FOR UPDATE, so:
 *  - the consumer's own account lock (AccountRepository.findByIdForUpdate) must not be
 *    FOR UPDATE, or every transfer would stall new POSTs for the same accounts;
 *  - a FOR UPDATE held by something else (a manual fix, a script) can still make the insert
 *    wait past lock_timeout, and the client must then get a 503 service-busy problem it can
 *    retry, not a bare 500.
 */
class PaymentCreationUnderLockIT extends AbstractIntegrationTest {

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
    private TransactionTemplate transactionTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    void postIsA503ServiceBusyProblemWhenTheAccountIsHeldForUpdatePastTheLockTimeout() throws Exception {
        Account from = accountRepository.save(new Account(id("SRC"), new BigDecimal("100.00"), "USD"));
        Account to = accountRepository.save(new Account(id("DST"), new BigDecimal("0.00"), "USD"));
        String paymentId = id("PAY");

        ResponseEntity<String> response = whileHolding(
                () -> jdbcTemplate.queryForObject("SELECT id FROM accounts WHERE id = ? FOR UPDATE", String.class,
                        from.getId()),
                () -> post(new PaymentRequest(paymentId, from.getId(), to.getId(), new BigDecimal("10.00"), "USD")));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        assertThat(response.getHeaders().getContentType()).isNotNull();
        assertThat(response.getHeaders().getContentType().isCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON)).isTrue();
        assertThat(response.getHeaders().getFirst("Retry-After")).isEqualTo("1");
        JsonNode problem = objectMapper.readTree(response.getBody());
        assertThat(problem.path("type").asText()).isEqualTo(TYPE_BASE + "service-busy");
        assertThat(problem.path("status").asInt()).isEqualTo(503);
        assertThat(problem.path("instance").asText()).isEqualTo("/payments");
        // The insert rolled back, so a retry creates the payment normally.
        assertThat(paymentRepository.findById(paymentId)).isEmpty();
        assertThat(post(new PaymentRequest(paymentId, from.getId(), to.getId(), new BigDecimal("10.00"), "USD"))
                .getStatusCode()).isEqualTo(HttpStatus.CREATED);
    }

    /**
     * Holds the account through the exact repository call PaymentService.process() uses, then
     * POSTs a payment for it. With FOR UPDATE the POST would wait for the lock (and fail after
     * lock_timeout); with FOR NO KEY UPDATE it is created at once.
     */
    @Test
    void postSucceedsImmediatelyWhileTheConsumerLockIsHeldOnTheAccount() throws Exception {
        Account from = accountRepository.save(new Account(id("SRC"), new BigDecimal("100.00"), "USD"));
        Account to = accountRepository.save(new Account(id("DST"), new BigDecimal("0.00"), "USD"));
        String paymentId = id("PAY");

        long start = System.nanoTime();
        ResponseEntity<String> response = whileHolding(
                () -> accountRepository.findByIdForUpdate(from.getId()).orElseThrow(),
                () -> post(new PaymentRequest(paymentId, from.getId(), to.getId(), new BigDecimal("10.00"), "USD")));
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(elapsedMs).as("POST should not wait for the consumer's lock").isLessThan(3_000);
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(paymentRepository.findById(paymentId).orElseThrow().getStatus())
                        .isEqualTo(PaymentStatus.PROCESSED));
    }

    /** Runs lock in a transaction on another thread, runs action while it is held, then releases it. */
    private <T> T whileHolding(Runnable lock, java.util.function.Supplier<T> action) throws Exception {
        CountDownLatch held = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CompletableFuture<Void> holder = CompletableFuture.runAsync(() -> transactionTemplate.executeWithoutResult(status -> {
            lock.run();
            held.countDown();
            try {
                release.await(60, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));
        try {
            assertThat(held.await(10, TimeUnit.SECONDS)).isTrue();
            return action.get();
        } finally {
            release.countDown();
            holder.get(30, TimeUnit.SECONDS);
        }
    }

    private ResponseEntity<String> post(PaymentRequest request) {
        return restTemplate.postForEntity("/payments", request, String.class);
    }

    private static String id(String prefix) {
        return prefix + "-" + UUID.randomUUID();
    }
}
