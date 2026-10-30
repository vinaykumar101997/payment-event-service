package com.example.paymentevent.metrics;

import com.example.paymentevent.domain.PaymentStatus;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PaymentMetricsTest {

    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final PaymentMetrics metrics = new PaymentMetrics(registry);

    @AfterEach
    void clearSynchronization() {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.clearSynchronization();
        }
    }

    @Test
    void everyFinalStatusIsRegisteredAtZero() {
        for (PaymentStatus status : PaymentMetrics.FINAL_STATUSES) {
            assertThat(finalStatus(status)).as(status.name()).isZero();
        }
        assertThat(registry.find("payments.final.status").counters()).hasSize(4);
    }

    @Test
    void outsideATransactionTheStatusIsCountedAtOnce() {
        metrics.finalStatusReached(PaymentStatus.PROCESSED);

        assertThat(finalStatus(PaymentStatus.PROCESSED)).isEqualTo(1);
        assertThat(finalStatus(PaymentStatus.FAILED)).isZero();
    }

    @Test
    void insideATransactionTheStatusIsCountedOnlyAfterCommit() {
        TransactionSynchronizationManager.initSynchronization();

        metrics.finalStatusReached(PaymentStatus.FAILED);
        assertThat(finalStatus(PaymentStatus.FAILED)).isZero();

        TransactionSynchronizationManager.getSynchronizations().forEach(TransactionSynchronization::afterCommit);
        assertThat(finalStatus(PaymentStatus.FAILED)).isEqualTo(1);
    }

    @Test
    void aRolledBackTransactionIsNotCounted() {
        TransactionSynchronizationManager.initSynchronization();

        metrics.finalStatusReached(PaymentStatus.POISONED);
        TransactionSynchronizationManager.getSynchronizations()
                .forEach(s -> s.afterCompletion(TransactionSynchronization.STATUS_ROLLED_BACK));

        assertThat(finalStatus(PaymentStatus.POISONED)).isZero();
    }

    @Test
    void aStatusThatIsNotFinalIsRefused() {
        assertThatThrownBy(() -> metrics.finalStatusReached(PaymentStatus.HELD))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void publishFailuresAreTaggedByTopicAndOverdueHoldsAreCounted() {
        metrics.outboxPublishFailed("payments");
        metrics.outboxPublishFailed("payments");
        metrics.outboxPublishFailed("transactions");
        metrics.onOverdue("p1", Duration.ofHours(2));

        assertThat(registry.get("payments.outbox.publish.failures").tag("topic", "payments").counter().count())
                .isEqualTo(2);
        assertThat(registry.get("payments.outbox.publish.failures").tag("topic", "transactions").counter().count())
                .isEqualTo(1);
        assertThat(registry.get("payments.screening.overdue").counter().count()).isEqualTo(1);
    }

    private double finalStatus(PaymentStatus status) {
        return registry.get("payments.final.status").tag("status", status.name()).counter().count();
    }
}
