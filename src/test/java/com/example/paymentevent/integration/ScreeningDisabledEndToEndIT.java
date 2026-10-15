package com.example.paymentevent.integration;

import com.example.paymentevent.api.PaymentRequest;
import com.example.paymentevent.domain.Account;
import com.example.paymentevent.domain.PaymentStatus;
import com.example.paymentevent.repository.AccountRepository;
import com.example.paymentevent.repository.PaymentRepository;
import com.example.paymentevent.screening.ScreeningVerdictConsumer;
import com.example.paymentevent.service.ReconciliationService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.ApplicationContext;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * S7, screening off (the default): a payment goes RECEIVED -> PROCESSED exactly as before
 * Stage 2. No hold, no screening request, no verdict listener, and the ledger reconciles.
 */
class ScreeningDisabledEndToEndIT extends AbstractIntegrationTest {

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

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private ApplicationContext context;

    @Test
    void withScreeningOffThePaymentFlowIsUnchanged() throws Exception {
        Account from = accountRepository.save(new Account("SRC-" + UUID.randomUUID(), new BigDecimal("100.00"), "USD"));
        Account to = accountRepository.save(new Account("DST-" + UUID.randomUUID(), new BigDecimal("0.00"), "USD"));
        String paymentId = "PAY-" + UUID.randomUUID();

        ResponseEntity<String> created = restTemplate.postForEntity("/payments",
                new PaymentRequest(paymentId, from.getId(), to.getId(), new BigDecimal("30.00"), "USD"), String.class);

        assertThat(created.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(objectMapper.readTree(created.getBody()).path("status").asText()).isEqualTo("RECEIVED");
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(paymentRepository.findById(paymentId).orElseThrow().getStatus())
                        .isEqualTo(PaymentStatus.PROCESSED));
        assertThat(accountRepository.findById(from.getId()).orElseThrow().getBalance()).isEqualByComparingTo("70.00");
        assertThat(accountRepository.findById(to.getId()).orElseThrow().getBalance()).isEqualByComparingTo("30.00");

        assertThat(jdbcTemplate.queryForList("SELECT topic FROM outbox_events WHERE message_key = ? ORDER BY id",
                String.class, paymentId)).containsExactly("payments", "transactions");
        assertThat(jdbcTemplate.queryForObject("SELECT count(*) FROM screening_verdicts WHERE payment_id = ?",
                Integer.class, paymentId)).isZero();
        assertThat(context.getBeanProvider(ScreeningVerdictConsumer.class).getIfAvailable()).isNull();
        assertThat(reconciliationService.reconcile().isClean()).isTrue();
    }
}
