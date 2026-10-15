package com.example.paymentevent.screening;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * Never auto-approves. Finds payments HELD longer than payments.screening.timeout (counted from
 * created_at, which is when they were held), sets overdue_at, logs a WARN line and calls every
 * HeldPaymentOverdueHook - once per payment. It never changes status: an overdue payment
 * stays HELD until a verdict or an officer decides. Releasing on a timeout would let any
 * payment through by making screening slow.
 *
 * Safe on several instances: the mark is one UPDATE over rows selected FOR UPDATE SKIP
 * LOCKED and WHERE overdue_at IS NULL, so a payment is marked, logged and reported by exactly
 * one sweep. Hooks run after that UPDATE has committed.
 */
@Component
public class HeldPaymentSweeper {

    private static final Logger log = LoggerFactory.getLogger(HeldPaymentSweeper.class);
    private static final int BATCH_SIZE = 100;

    private static final String MARK_OVERDUE_SQL =
            "UPDATE payments SET overdue_at = ? WHERE payment_id IN ("
                    + "  SELECT payment_id FROM payments"
                    + "  WHERE status = 'HELD' AND overdue_at IS NULL AND created_at < ?"
                    + "  ORDER BY created_at LIMIT ? FOR UPDATE SKIP LOCKED"
                    + ") RETURNING payment_id, created_at";

    private final JdbcTemplate jdbcTemplate;
    private final TransactionTemplate transactionTemplate;
    private final List<HeldPaymentOverdueHook> hooks;
    private final Duration timeout;

    public HeldPaymentSweeper(JdbcTemplate jdbcTemplate, TransactionTemplate transactionTemplate,
                              List<HeldPaymentOverdueHook> hooks,
                              @Value("${payments.screening.timeout:PT1H}") Duration timeout) {
        this.jdbcTemplate = jdbcTemplate;
        this.transactionTemplate = transactionTemplate;
        this.hooks = List.copyOf(hooks);
        this.timeout = timeout;
    }

    @Scheduled(fixedDelayString = "${payments.screening.overdue-sweep-interval-ms:60000}")
    public void sweep() {
        List<Overdue> marked;
        do {
            Instant now = Instant.now();
            marked = transactionTemplate.execute(status -> jdbcTemplate.query(MARK_OVERDUE_SQL,
                    (rs, rowNum) -> new Overdue(rs.getString("payment_id"), rs.getTimestamp("created_at").toInstant()),
                    Timestamp.from(now), Timestamp.from(now.minus(timeout)), BATCH_SIZE));
            for (Overdue payment : marked) {
                Duration heldFor = Duration.between(payment.heldSince(), now);
                log.warn("Payment {} has been HELD for {} (timeout {}); marked overdue. It stays HELD until a "
                        + "screening verdict or an officer decides.", payment.paymentId(), heldFor, timeout);
                notifyHooks(payment.paymentId(), heldFor);
            }
        } while (marked.size() == BATCH_SIZE);
    }

    private void notifyHooks(String paymentId, Duration heldFor) {
        for (HeldPaymentOverdueHook hook : hooks) {
            try {
                hook.onOverdue(paymentId, heldFor);
            } catch (RuntimeException ex) {
                log.error("Overdue hook {} failed for payment {}", hook.getClass().getName(), paymentId, ex);
            }
        }
    }

    private record Overdue(String paymentId, Instant heldSince) {
    }
}
