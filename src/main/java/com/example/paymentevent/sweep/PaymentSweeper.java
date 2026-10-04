package com.example.paymentevent.sweep;

import com.example.paymentevent.domain.Payment;
import com.example.paymentevent.domain.PaymentStatus;
import com.example.paymentevent.kafka.PaymentEvent;
import com.example.paymentevent.outbox.OutboxWriter;
import com.example.paymentevent.repository.PaymentRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * Safety net, no longer the primary delivery mechanism. The dual-write gap it was built for
 * ("insert Payment row" then "publish to Kafka", non-atomically) is closed by the
 * transactional outbox: createPayment() writes the "payments" event in the same transaction
 * as the row and OutboxRelay retries it until it's published.
 *
 * What's left for it: a payment whose event WAS delivered but whose processing kept failing
 * with a retryable error (e.g. the DB unreachable for longer than the listener's ~10s
 * backoff) is rolled back to RECEIVED and its message lands on the DLT - nothing would ever
 * process it again. Also RECEIVED rows created before the outbox migration, which have no
 * outbox row. This sweeper re-enqueues anything still RECEIVED after STALE_THRESHOLD, on
 * the assumption that normal processing takes milliseconds, not minutes.
 *
 * It never publishes to Kafka itself: it writes a new "payments" event through OutboxWriter,
 * so OutboxRelay is the single publish path (with its retries and dead-row handling).
 *
 * Safe to run on several instances at once. Candidates are claimed by one UPDATE that sets
 * last_swept_at on rows selected FOR UPDATE SKIP LOCKED, committed before any enqueueing:
 * a concurrent sweep skips rows another one is claiming, and once committed the cooldown
 * (last_swept_at) stops anyone re-claiming them for STALE_THRESHOLD. Each claimed payment is
 * then enqueued in its own transaction, so one failure is logged and skipped rather than
 * aborting the batch; that payment is simply picked up again after the cooldown.
 */
@Component
public class PaymentSweeper {

    private static final Logger log = LoggerFactory.getLogger(PaymentSweeper.class);
    private static final Duration STALE_THRESHOLD = Duration.ofSeconds(60);
    private static final int BATCH_SIZE = 100;

    /**
     * Payments with a "payments" outbox row still pending (not yet published, not dead) are
     * skipped: the relay is already on it, and re-enqueueing during a broker outage would just
     * pile up duplicate rows.
     */
    private static final String CLAIM_STALE_SQL =
            "UPDATE payments SET last_swept_at = ? WHERE payment_id IN ("
                    + "  SELECT p.payment_id FROM payments p"
                    + "  WHERE p.status = 'RECEIVED' AND p.created_at < ?"
                    + "    AND (p.last_swept_at IS NULL OR p.last_swept_at < ?)"
                    + "    AND NOT EXISTS (SELECT 1 FROM outbox_events o"
                    + "                    WHERE o.message_key = p.payment_id AND o.topic = 'payments'"
                    + "                      AND o.published_at IS NULL AND o.dead_at IS NULL)"
                    + "  ORDER BY p.created_at LIMIT ? FOR UPDATE SKIP LOCKED"
                    + ") RETURNING payment_id";

    private final JdbcTemplate jdbcTemplate;
    private final TransactionTemplate transactionTemplate;
    private final PaymentRepository paymentRepository;
    private final OutboxWriter outboxWriter;

    public PaymentSweeper(JdbcTemplate jdbcTemplate, TransactionTemplate transactionTemplate,
                          PaymentRepository paymentRepository, OutboxWriter outboxWriter) {
        this.jdbcTemplate = jdbcTemplate;
        this.transactionTemplate = transactionTemplate;
        this.paymentRepository = paymentRepository;
        this.outboxWriter = outboxWriter;
    }

    @Scheduled(fixedDelayString = "${payments.sweeper.interval-ms:30000}")
    public void sweep() {
        List<String> claimed = claimStalePayments(Instant.now());

        for (String paymentId : claimed) {
            try {
                transactionTemplate.executeWithoutResult(status -> reenqueueIfStillReceived(paymentId));
            } catch (RuntimeException ex) {
                log.error("Could not re-enqueue stale payment {}; it will be retried after the {} cooldown",
                        paymentId, STALE_THRESHOLD, ex);
            }
        }
    }

    private List<String> claimStalePayments(Instant now) {
        Timestamp threshold = Timestamp.from(now.minus(STALE_THRESHOLD));
        return transactionTemplate.execute(status -> jdbcTemplate.queryForList(
                CLAIM_STALE_SQL, String.class, Timestamp.from(now), threshold, threshold, BATCH_SIZE));
    }

    /** Re-checked inside its own transaction: a consumer may have claimed it since the sweep's claim. */
    private void reenqueueIfStillReceived(String paymentId) {
        Payment payment = paymentRepository.findById(paymentId).orElse(null);
        if (payment == null || payment.getStatus() != PaymentStatus.RECEIVED) {
            return;
        }
        log.warn("Sweeping stale RECEIVED payment {} (created at {}) - re-enqueueing its \"payments\" event",
                paymentId, payment.getCreatedAt());
        outboxWriter.enqueuePaymentEvent(PaymentEvent.from(payment));
    }
}
