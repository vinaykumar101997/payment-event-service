package com.example.paymentevent.kafka;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Message contract for the "transactions" topic: written to the outbox in the same
 * transaction that debits/credits the payment, then published by OutboxRelay. Delivery is
 * at-least-once (see OutboxRelay) — consumers of this topic must dedupe by paymentId.
 */
public record TransactionEvent(
        String paymentId,
        String fromAccount,
        String toAccount,
        BigDecimal amount,
        String currency,
        Instant processedAt) {
}
