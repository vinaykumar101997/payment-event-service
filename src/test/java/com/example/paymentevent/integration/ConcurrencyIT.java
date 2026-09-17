package com.example.paymentevent.integration;

import com.example.paymentevent.api.PaymentRequest;
import com.example.paymentevent.api.PaymentResponse;
import com.example.paymentevent.domain.Account;
import com.example.paymentevent.domain.Payment;
import com.example.paymentevent.domain.PaymentStatus;
import com.example.paymentevent.repository.AccountRepository;
import com.example.paymentevent.repository.PaymentRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class ConcurrencyIT extends AbstractIntegrationTest {

    private static final BigDecimal AMOUNT = new BigDecimal("10.00");

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private AccountRepository accountRepository;

    @Autowired
    private PaymentRepository paymentRepository;

    /**
     * Balance only covers 60 of the 100 payments. Every debit is validated for sufficient
     * funds under the same pessimistic lock it's applied with, so the outcome should be
     * exactly 60 PROCESSED / 40 FAILED, with the source balance landing at exactly zero -
     * not negative, and not short of zero from a lost update.
     */
    @Test
    void partialFundsAllowsExactlySixtyOfOneHundredConcurrentPayments() throws InterruptedException {
        Account source = accountRepository.save(new Account(id("SRC"), new BigDecimal("600.00"), "USD"));
        Account dest = accountRepository.save(new Account(id("DST"), new BigDecimal("0.00"), "USD"));

        List<String> paymentIds = fireConcurrentPayments(source.getId(), dest.getId(), 100);

        awaitAllTerminal(paymentIds);

        List<Payment> payments = paymentRepository.findAllById(paymentIds);
        long processed = payments.stream().filter(p -> p.getStatus() == PaymentStatus.PROCESSED).count();
        long failed = payments.stream().filter(p -> p.getStatus() == PaymentStatus.FAILED).count();

        assertThat(processed).isEqualTo(60);
        assertThat(failed).isEqualTo(40);

        BigDecimal finalSourceBalance = accountRepository.findById(source.getId()).orElseThrow().getBalance();
        assertThat(finalSourceBalance).isEqualByComparingTo("0.00");
        assertThat(finalSourceBalance.signum()).isGreaterThanOrEqualTo(0);

        assertThat(accountRepository.findById(dest.getId()).orElseThrow().getBalance())
                .isEqualByComparingTo("600.00");
    }

    /**
     * 50 payments A->B and 50 payments B->A, fired concurrently and interleaved, so roughly
     * half the consumer threads try to lock (A, B) and half try (B, A). If the deterministic
     * lock-ordering in PaymentService.lockAccountsInOrder didn't hold, this would deadlock
     * or time out instead of completing. The strongest correctness check isn't the
     * individual balances (which depend on how the 50/50 split nets out) but that total
     * money is conserved across the two accounts.
     */
    @Test
    void concurrentTransfersInBothDirectionsConserveTotalMoney() throws InterruptedException {
        Account a = accountRepository.save(new Account(id("A"), new BigDecimal("1000.00"), "USD"));
        Account b = accountRepository.save(new Account(id("B"), new BigDecimal("1000.00"), "USD"));
        BigDecimal totalBefore = a.getBalance().add(b.getBalance());

        List<PaymentRequest> requests = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            requests.add(new PaymentRequest(id("AB"), a.getId(), b.getId(), AMOUNT, "USD"));
            requests.add(new PaymentRequest(id("BA"), b.getId(), a.getId(), AMOUNT, "USD"));
        }

        ExecutorService executor = Executors.newFixedThreadPool(20);
        try {
            List<java.util.concurrent.Callable<Void>> tasks = requests.stream()
                    .<java.util.concurrent.Callable<Void>>map(request -> () -> {
                        restTemplate.postForEntity("/payments", request, PaymentResponse.class);
                        return null;
                    })
                    .collect(Collectors.toList());
            executor.invokeAll(tasks);
        } finally {
            executor.shutdown();
        }

        List<String> paymentIds = requests.stream().map(PaymentRequest::paymentId).collect(Collectors.toList());
        awaitAllTerminal(paymentIds);

        List<Payment> payments = paymentRepository.findAllById(paymentIds);
        assertThat(payments).allMatch(p -> p.getStatus() == PaymentStatus.PROCESSED);

        BigDecimal totalAfter = accountRepository.findById(a.getId()).orElseThrow().getBalance()
                .add(accountRepository.findById(b.getId()).orElseThrow().getBalance());
        assertThat(totalAfter).isEqualByComparingTo(totalBefore);
    }

    private List<String> fireConcurrentPayments(String fromAccount, String toAccount, int count)
            throws InterruptedException {
        List<String> paymentIds = IntStream.range(0, count).mapToObj(i -> id("PAY")).collect(Collectors.toList());

        ExecutorService executor = Executors.newFixedThreadPool(20);
        try {
            List<java.util.concurrent.Callable<Void>> tasks = paymentIds.stream()
                    .<java.util.concurrent.Callable<Void>>map(paymentId -> () -> {
                        PaymentRequest request = new PaymentRequest(paymentId, fromAccount, toAccount, AMOUNT, "USD");
                        restTemplate.postForEntity("/payments", request, PaymentResponse.class);
                        return null;
                    })
                    .collect(Collectors.toList());
            executor.invokeAll(tasks);
        } finally {
            executor.shutdown();
        }

        return paymentIds;
    }

    private void awaitAllTerminal(List<String> paymentIds) {
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(300)).untilAsserted(() -> {
            List<Payment> payments = paymentRepository.findAllById(paymentIds);
            assertThat(payments).hasSize(paymentIds.size());
            assertThat(payments).allMatch(p -> p.getStatus() == PaymentStatus.PROCESSED
                    || p.getStatus() == PaymentStatus.FAILED);
        });
    }

    private static String id(String prefix) {
        return prefix + "-" + UUID.randomUUID();
    }
}
