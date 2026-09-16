package com.example.paymentevent.kafka;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Message contract for the "transactions" topic: published after a payment has been
 * successfully debited/credited. Delivery is at-least-once (see PaymentProcessingResult's
 * ALREADY_PROCESSED case) — consumers of this topic must dedupe by paymentId.
 */
public record TransactionEvent(
        String paymentId,
        String fromAccount,
        String toAccount,
        BigDecimal amount,
        String currency,
        Instant processedAt) {
}
