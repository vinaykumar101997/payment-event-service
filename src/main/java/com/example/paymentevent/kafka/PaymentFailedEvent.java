package com.example.paymentevent.kafka;

import com.example.paymentevent.domain.Payment;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Message contract for the "payment.failed" topic: written to the outbox in the same
 * transaction that marks a payment FAILED (insufficient funds, currency mismatch, unknown
 * account, or an event that didn't match the stored payment) or REJECTED (screening or an
 * officer), then published by OutboxRelay.
 * No money moved. At-least-once, like every outbox event - consumers dedupe by paymentId.
 */
public record PaymentFailedEvent(
        String paymentId,
        String fromAccount,
        String toAccount,
        BigDecimal amount,
        String currency,
        String reason,
        Instant failedAt) {

    public static PaymentFailedEvent from(Payment payment) {
        return new PaymentFailedEvent(payment.getPaymentId(), payment.getFromAccount(), payment.getToAccount(),
                payment.getAmount(), payment.getCurrency(), payment.getFailureReason(), payment.getProcessedAt());
    }
}
