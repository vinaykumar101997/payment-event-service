package com.example.paymentevent.integration;

import com.example.paymentevent.domain.Account;
import com.example.paymentevent.domain.Payment;
import com.example.paymentevent.repository.AccountRepository;
import com.example.paymentevent.repository.PaymentRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * The invariants Java already enforces (balance never negative, amount positive, distinct
 * accounts) are also enforced by Postgres CHECK constraints (V4__db_constraints.sql), so a
 * code path that bypasses the controller/PaymentService validation - a bug, a script, a
 * manual fix - still can't write a row that breaks them. Every write here goes straight
 * through a repository or JdbcTemplate, skipping all Java validation.
 *
 * Each violating write runs in a transaction that is always rolled back, so even if a
 * constraint were missing the bad row never reaches the database shared by every IT class.
 */
class DatabaseConstraintsIT extends AbstractIntegrationTest {

    private static final String CHECK_VIOLATION = "23514";

    @Autowired
    private AccountRepository accountRepository;

    @Autowired
    private PaymentRepository paymentRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private TransactionTemplate transactionTemplate;

    @Test
    void accountCannotBeCreatedWithNegativeBalance() {
        Throwable thrown = attemptAndRollBack(() ->
                accountRepository.saveAndFlush(new Account(id("NEG"), new BigDecimal("-0.01"), "USD")));

        assertCheckViolation(thrown, "accounts_balance_non_negative");
    }

    @Test
    void accountBalanceCannotBeDebitedBelowZero() {
        Account account = accountRepository.save(new Account(id("ACC"), new BigDecimal("10.00"), "USD"));

        Throwable viaJpa = attemptAndRollBack(() -> {
            Account loaded = accountRepository.findById(account.getId()).orElseThrow();
            loaded.debit(new BigDecimal("10.01"));
            accountRepository.saveAndFlush(loaded);
        });
        Throwable viaSql = attemptAndRollBack(() ->
                jdbcTemplate.update("UPDATE accounts SET balance = -5.00 WHERE id = ?", account.getId()));

        assertCheckViolation(viaJpa, "accounts_balance_non_negative");
        assertCheckViolation(viaSql, "accounts_balance_non_negative");
        assertThat(accountRepository.findById(account.getId()).orElseThrow().getBalance())
                .isEqualByComparingTo("10.00");
    }

    @Test
    void paymentAmountMustBePositive() {
        Account from = accountRepository.save(new Account(id("SRC"), new BigDecimal("100.00"), "USD"));
        Account to = accountRepository.save(new Account(id("DST"), new BigDecimal("0.00"), "USD"));

        Throwable zero = attemptAndRollBack(() -> paymentRepository.saveAndFlush(
                new Payment(id("PAY"), from.getId(), to.getId(), new BigDecimal("0.00"), "USD")));
        Throwable negative = attemptAndRollBack(() -> paymentRepository.saveAndFlush(
                new Payment(id("PAY"), from.getId(), to.getId(), new BigDecimal("-25.00"), "USD")));

        assertCheckViolation(zero, "payments_amount_positive");
        assertCheckViolation(negative, "payments_amount_positive");
    }

    @Test
    void paymentFromAndToAccountsMustDiffer() {
        Account account = accountRepository.save(new Account(id("SELF"), new BigDecimal("100.00"), "USD"));

        Throwable thrown = attemptAndRollBack(() -> paymentRepository.saveAndFlush(
                new Payment(id("PAY"), account.getId(), account.getId(), new BigDecimal("10.00"), "USD")));

        assertCheckViolation(thrown, "payments_distinct_accounts");
    }

    private Throwable attemptAndRollBack(Runnable write) {
        return catchThrowable(() -> transactionTemplate.executeWithoutResult(status -> {
            status.setRollbackOnly();
            write.run();
        }));
    }

    private static void assertCheckViolation(Throwable thrown, String constraintName) {
        assertThat(thrown)
                .as("the database should reject the write via %s", constraintName)
                .isInstanceOf(DataIntegrityViolationException.class);
        assertThat(sqlStateOf(thrown)).isEqualTo(CHECK_VIOLATION);
        assertThat(thrown).rootCause().hasMessageContaining(constraintName);
    }

    private static String sqlStateOf(Throwable ex) {
        for (Throwable t = ex; t != null; t = t.getCause()) {
            if (t instanceof SQLException sqlException && sqlException.getSQLState() != null) {
                return sqlException.getSQLState();
            }
        }
        return null;
    }

    private static String id(String prefix) {
        return prefix + "-" + UUID.randomUUID();
    }
}
