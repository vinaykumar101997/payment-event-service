package com.example.paymentevent.integration;

import com.example.paymentevent.domain.Account;
import com.example.paymentevent.repository.AccountRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.PessimisticLockingFailureException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DatabaseTimeoutsIT extends AbstractIntegrationTest {

    @Autowired
    private AccountRepository accountRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private TransactionTemplate transactionTemplate;

    @Test
    void pooledConnectionsCarryLockAndStatementTimeouts() {
        assertThat(jdbcTemplate.queryForObject("SHOW lock_timeout", String.class)).isEqualTo("5s");
        assertThat(jdbcTemplate.queryForObject("SHOW statement_timeout", String.class)).isEqualTo("30s");
    }

    /**
     * Another transaction holds an account row lock and doesn't let go (a stuck consumer, a
     * long manual UPDATE). Without lock_timeout, PaymentService's SELECT ... FOR UPDATE on that
     * row would wait indefinitely, pinning a consumer thread and a pooled connection. With it,
     * the wait fails fast with a locking exception - which the listener treats as a retryable
     * failure - instead of hanging. The test gives it 15s; lock_timeout is 5s.
     */
    @Test
    void lockWaitOnAHeldAccountRowFailsFastInsteadOfHanging() throws Exception {
        Account account = accountRepository.save(new Account("LOCKED-" + UUID.randomUUID(), new BigDecimal("100.00"), "USD"));
        CountDownLatch lockHeld = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        CompletableFuture<Void> holder = CompletableFuture.runAsync(() -> transactionTemplate.executeWithoutResult(status -> {
            jdbcTemplate.queryForObject("SELECT id FROM accounts WHERE id = ? FOR UPDATE", String.class, account.getId());
            lockHeld.countDown();
            try {
                release.await(60, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));

        try {
            assertThat(lockHeld.await(10, TimeUnit.SECONDS)).isTrue();
            long start = System.nanoTime();
            CompletableFuture<Object> waiter = CompletableFuture.supplyAsync(() -> transactionTemplate.execute(status ->
                    accountRepository.findByIdForUpdate(account.getId())));

            assertThatThrownBy(() -> waiter.get(15, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(PessimisticLockingFailureException.class);
            assertThat(TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start)).isLessThan(10);
        } finally {
            release.countDown();
            holder.get(30, TimeUnit.SECONDS);
        }
    }
}
