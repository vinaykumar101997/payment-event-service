package com.example.paymentevent.metrics;

import com.example.paymentevent.domain.PaymentStatus;
import com.example.paymentevent.screening.HeldPaymentOverdueHook;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.Duration;
import java.util.EnumMap;
import java.util.Map;
import java.util.Set;

/**
 * Event counters, exposed on /actuator/prometheus. Tags are fixed, small sets (a status, a
 * topic): never a payment id, account id or amount, so the number of series stays bounded.
 *
 *   payments.final.status{status}           payments reaching PROCESSED, FAILED, POISONED or REJECTED
 *   payments.screening.overdue              HELD payments the sweeper marked overdue
 *   payments.outbox.publish.failures{topic} failed Kafka publishes by OutboxRelay
 *
 * A final status is counted when the transaction that wrote it commits, so a rolled-back
 * attempt (a lock timeout, a crash before commit) is never counted. Outside a transaction it
 * is counted at once. The backlog gauges are in BacklogMetrics.
 */
@Component
public class PaymentMetrics implements HeldPaymentOverdueHook {

    static final Set<PaymentStatus> FINAL_STATUSES =
            Set.of(PaymentStatus.PROCESSED, PaymentStatus.FAILED, PaymentStatus.POISONED, PaymentStatus.REJECTED);

    private final MeterRegistry registry;
    private final Map<PaymentStatus, Counter> finalStatus = new EnumMap<>(PaymentStatus.class);
    private final Counter overdue;

    public PaymentMetrics(MeterRegistry registry) {
        this.registry = registry;
        // Registered up front so every status shows as 0 before its first payment.
        for (PaymentStatus status : FINAL_STATUSES) {
            finalStatus.put(status, Counter.builder("payments.final.status")
                    .description("Payments that reached a final status")
                    .tag("status", status.name())
                    .register(registry));
        }
        this.overdue = Counter.builder("payments.screening.overdue")
                .description("HELD payments marked overdue by the sweeper")
                .register(registry);
    }

    /** Counts a payment reaching a final status, once the current transaction (if any) commits. */
    public void finalStatusReached(PaymentStatus status) {
        Counter counter = finalStatus.get(status);
        if (counter == null) {
            throw new IllegalArgumentException(status + " is not a final status");
        }
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    counter.increment();
                }
            });
        } else {
            counter.increment();
        }
    }

    /** Called by OutboxRelay for each failed publish; topic is one of the few outbox topics. */
    public void outboxPublishFailed(String topic) {
        Counter.builder("payments.outbox.publish.failures")
                .description("Failed Kafka publishes from the outbox relay")
                .tag("topic", topic)
                .register(registry)
                .increment();
    }

    /** HeldPaymentSweeper calls this once per payment it marks overdue. */
    @Override
    public void onOverdue(String paymentId, Duration heldFor) {
        overdue.increment();
    }
}
