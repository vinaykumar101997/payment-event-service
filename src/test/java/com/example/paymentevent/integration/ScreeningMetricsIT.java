package com.example.paymentevent.integration;

import com.example.paymentevent.api.PaymentRequest;
import com.example.paymentevent.domain.Account;
import com.example.paymentevent.domain.PaymentStatus;
import com.example.paymentevent.metrics.BacklogMetrics;
import com.example.paymentevent.repository.AccountRepository;
import com.example.paymentevent.repository.PaymentRepository;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;

import java.math.BigDecimal;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * With screening on, a payment created through the API raises the held gauge, and an officer
 * reject lowers it again and is counted as REJECTED.
 */
class ScreeningMetricsIT extends AbstractScreeningIntegrationTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private MeterRegistry registry;

    @Autowired
    private BacklogMetrics backlogMetrics;

    @Autowired
    private AccountRepository accountRepository;

    @Autowired
    private PaymentRepository paymentRepository;

    @Test
    void heldGaugeFollowsTheHoldAndARejectIsCounted() {
        Account from = accountRepository.save(new Account(id("SRC"), new BigDecimal("100.00"), "USD"));
        Account to = accountRepository.save(new Account(id("DST"), new BigDecimal("0.00"), "USD"));
        String paymentId = id("PAY");
        backlogMetrics.refresh();
        double heldBefore = held();
        double rejectedBefore = rejected();

        assertThat(restTemplate.postForEntity("/payments",
                new PaymentRequest(paymentId, from.getId(), to.getId(), new BigDecimal("10.00"), "USD"),
                String.class).getStatusCode().value()).isEqualTo(HttpStatus.CREATED.value());
        assertThat(paymentRepository.findById(paymentId).orElseThrow().getStatus()).isEqualTo(PaymentStatus.HELD);
        backlogMetrics.refresh();
        assertThat(held()).isEqualTo(heldBefore + 1);

        assertThat(restTemplate.postForEntity("/payments/" + paymentId + "/reject",
                Map.of("officer", "alice", "note", "confirmed fraud"), String.class).getStatusCode().value())
                .isEqualTo(HttpStatus.OK.value());
        assertThat(rejected()).isEqualTo(rejectedBefore + 1);
        backlogMetrics.refresh();
        assertThat(held()).isEqualTo(heldBefore);
    }

    private double held() {
        return registry.get("payments.screening.held").gauge().value();
    }

    private double rejected() {
        return registry.get("payments.final.status").tag("status", PaymentStatus.REJECTED.name()).counter().count();
    }

    private static String id(String prefix) {
        return prefix + "-" + UUID.randomUUID();
    }
}
