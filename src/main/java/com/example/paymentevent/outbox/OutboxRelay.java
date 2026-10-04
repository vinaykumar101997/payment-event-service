package com.example.paymentevent.outbox;

import com.example.paymentevent.domain.OutboxEvent;
import com.example.paymentevent.kafka.PaymentEvent;
import com.example.paymentevent.kafka.PaymentFailedEvent;
import com.example.paymentevent.kafka.PaymentProducer;
import com.example.paymentevent.kafka.TransactionEvent;
import com.example.paymentevent.repository.OutboxEventRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * Publishes outbox rows to Kafka. Each batch runs in one transaction: claim pending rows
 * with FOR UPDATE SKIP LOCKED, publish each through PaymentProducer (which blocks until the
 * broker acks), then mark it published - committed together at the end of the batch.
 *
 * This is the one place Kafka I/O happens inside a DB transaction, and it's deliberate: the
 * only locks held are on the claimed outbox rows, which nothing on the request or consumer
 * path ever locks (they only INSERT new rows), so a slow broker can delay publishing but can't
 * block a payment. Holding those locks for the publish is also exactly what makes the claim
 * safe across instances - a second relay's SKIP LOCKED claim passes over them.
 *
 * Delivery is at-least-once: a crash after the broker acked but before the batch commits
 * leaves the row unpublished, so it's sent again. Consumers dedupe by key (paymentId).
 *
 * A failed publish records the error and backs the row off exponentially (next_attempt_at),
 * so a row that keeps failing doesn't block the ones behind it. After outbox.relay.max-attempts
 * failures the row is marked dead (dead_at, logged at ERROR, never claimed again) instead of
 * being retried forever. After a few consecutive failures the batch stops early - the broker
 * is probably down, and every further send would just hold the transaction open until its
 * own timeout.
 */
@Component
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    static final int BATCH_SIZE = 100;
    private static final int MAX_CONSECUTIVE_FAILURES = 3;
    private static final Duration BASE_BACKOFF = Duration.ofMillis(500);

    private final OutboxEventRepository outboxEventRepository;
    private final PaymentProducer paymentProducer;
    private final ObjectMapper objectMapper;
    private final TransactionTemplate transactionTemplate;
    private final Duration maxBackoff;
    private final int maxAttempts;

    public OutboxRelay(OutboxEventRepository outboxEventRepository, PaymentProducer paymentProducer,
                       ObjectMapper objectMapper, TransactionTemplate transactionTemplate,
                       @Value("${outbox.relay.max-backoff-ms:30000}") long maxBackoffMs,
                       @Value("${outbox.relay.max-attempts:50}") int maxAttempts) {
        this.outboxEventRepository = outboxEventRepository;
        this.paymentProducer = paymentProducer;
        this.objectMapper = objectMapper;
        this.transactionTemplate = transactionTemplate;
        this.maxBackoff = Duration.ofMillis(maxBackoffMs);
        this.maxAttempts = maxAttempts;
    }

    @Scheduled(fixedDelayString = "${outbox.relay.poll-interval-ms:500}")
    public void relay() {
        // Keep draining while batches come back full, rather than publishing at most
        // BATCH_SIZE rows per poll interval under a burst.
        int published;
        do {
            published = relayBatch();
        } while (published == BATCH_SIZE);
    }

    /** Claims, publishes and marks one batch in a single transaction; returns how many were published. */
    public int relayBatch() {
        Integer published = transactionTemplate.execute(status ->
                publishAll(outboxEventRepository.claimPending(Instant.now(), BATCH_SIZE)));
        return published == null ? 0 : published;
    }

    private int publishAll(List<OutboxEvent> batch) {
        int published = 0;
        int consecutiveFailures = 0;
        for (OutboxEvent event : batch) {
            try {
                publish(event);
                event.markPublished();
                published++;
                consecutiveFailures = 0;
            } catch (RuntimeException ex) {
                Duration backoff = backoffAfter(event.getAttempts() + 1);
                if (event.recordFailure(ex.getMessage(), backoff, maxAttempts)) {
                    log.error("Outbox event {} (topic={}, key={}) is DEAD after {} failed publish attempts; "
                                    + "it will not be retried. Last error: {}",
                            event.getId(), event.getTopic(), event.getMessageKey(), event.getAttempts(),
                            event.getLastError(), ex);
                } else {
                    log.warn("Outbox publish failed for event {} (topic={}, key={}, attempt {}); retrying in {}",
                            event.getId(), event.getTopic(), event.getMessageKey(), event.getAttempts(), backoff, ex);
                }
                if (++consecutiveFailures >= MAX_CONSECUTIVE_FAILURES) {
                    log.warn("Stopping outbox batch after {} consecutive publish failures", consecutiveFailures);
                    break;
                }
            }
        }
        return published;
    }

    private void publish(OutboxEvent event) {
        switch (event.getTopic()) {
            case "payments" -> paymentProducer.publishPaymentEvent(read(event, PaymentEvent.class));
            case "transactions" -> paymentProducer.publishTransactionEvent(read(event, TransactionEvent.class));
            case "payment.failed" -> paymentProducer.publishPaymentFailedEvent(read(event, PaymentFailedEvent.class));
            default -> throw new IllegalStateException("No publisher for outbox topic " + event.getTopic());
        }
    }

    private <T> T read(OutboxEvent event, Class<T> type) {
        try {
            return objectMapper.readValue(event.getPayload(), type);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Unreadable outbox payload for event " + event.getId(), e);
        }
    }

    private Duration backoffAfter(int attempts) {
        Duration backoff = BASE_BACKOFF.multipliedBy(1L << Math.min(attempts - 1, 16));
        return backoff.compareTo(maxBackoff) > 0 ? maxBackoff : backoff;
    }
}
