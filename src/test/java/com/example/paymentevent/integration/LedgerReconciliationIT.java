package com.example.paymentevent.integration;

import com.example.paymentevent.api.PaymentRequest;
import com.example.paymentevent.api.PaymentResponse;
import com.example.paymentevent.domain.Account;
import com.example.paymentevent.domain.LedgerEntry;
import com.example.paymentevent.domain.Payment;
import com.example.paymentevent.domain.PaymentStatus;
import com.example.paymentevent.repository.AccountRepository;
import com.example.paymentevent.repository.LedgerEntryRepository;
import com.example.paymentevent.repository.PaymentRepository;
import com.example.paymentevent.service.ReconciliationReport;
import com.example.paymentevent.service.ReconciliationService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.jdbc.core.JdbcTemplate;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class LedgerReconciliationIT extends AbstractIntegrationTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private AccountRepository accountRepository;

    @Autowired
    private PaymentRepository paymentRepository;

    @Autowired
    private LedgerEntryRepository ledgerEntryRepository;

    @Autowired
    private ReconciliationService reconciliationService;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    void processedPaymentWritesBalancedDebitAndCreditEntries() {
        Account from = accountRepository.save(new Account(id("SRC"), new BigDecimal("1000.00"), "USD"));
        Account to = accountRepository.save(new Account(id("DST"), new BigDecimal("0.00"), "USD"));
        String paymentId = id("PAY");

        restTemplate.postForEntity("/payments",
                new PaymentRequest(paymentId, from.getId(), to.getId(), new BigDecimal("100.00"), "USD"),
                PaymentResponse.class);

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(paymentRepository.findById(paymentId).orElseThrow().getStatus())
                        .isEqualTo(PaymentStatus.PROCESSED));

        List<LedgerEntry> entries = ledgerEntryRepository.findByPaymentId(paymentId);
        assertThat(entries).hasSize(2);
        assertThat(entries).anySatisfy(e -> {
            assertThat(e.getAccountId()).isEqualTo(from.getId());
            assertThat(e.getAmount()).isEqualByComparingTo("-100.00");
        });
        assertThat(entries).anySatisfy(e -> {
            assertThat(e.getAccountId()).isEqualTo(to.getId());
            assertThat(e.getAmount()).isEqualByComparingTo("100.00");
        });

        assertThat(reconciliationService.reconcile().isClean()).isTrue();
    }

    /**
     * Proves the check isn't vacuous: a balance edited behind the ledger's back, and a stray
     * one-sided entry, must both be reported. Every IT class shares one database, so the
     * corruption is undone in finally - otherwise it would fail every later reconcile().
     */
    @Test
    void reconciliationReportsBalanceDriftAndOneSidedEntries() {
        Account drifted = accountRepository.save(new Account(id("DRIFT"), new BigDecimal("50.00"), "USD"));
        Account other = accountRepository.save(new Account(id("OTHER"), new BigDecimal("50.00"), "USD"));
        String paymentId = id("PAY");
        paymentRepository.saveAndFlush(new Payment(paymentId, drifted.getId(), other.getId(),
                new BigDecimal("7.00"), "USD"));
        jdbcTemplate.update("UPDATE payments SET status = 'FAILED' WHERE payment_id = ?", paymentId);

        try {
            jdbcTemplate.update("UPDATE accounts SET balance = balance + 5.00 WHERE id = ?", drifted.getId());
            jdbcTemplate.update("INSERT INTO ledger_entries (payment_id, account_id, amount, currency) "
                    + "VALUES (?, ?, 7.00, 'USD')", paymentId, other.getId());

            ReconciliationReport report = reconciliationService.reconcile();

            assertThat(report.isClean()).isFalse();
            assertThat(report.unbalancedPayments())
                    .extracting(ReconciliationReport.UnbalancedPayment::paymentId)
                    .containsExactly(paymentId);
            assertThat(report.accountDiscrepancies())
                    .extracting(ReconciliationReport.AccountDiscrepancy::accountId)
                    .containsExactlyInAnyOrder(drifted.getId(), other.getId());
        } finally {
            jdbcTemplate.update("DELETE FROM ledger_entries WHERE payment_id = ?", paymentId);
            jdbcTemplate.update("UPDATE accounts SET balance = opening_balance WHERE id = ?", drifted.getId());
        }

        assertThat(reconciliationService.reconcile().isClean()).isTrue();
    }

    private static String id(String prefix) {
        return prefix + "-" + UUID.randomUUID();
    }
}
