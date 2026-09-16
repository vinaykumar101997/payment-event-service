package com.example.paymentevent.kafka;

import java.math.BigDecimal;

/**
 * Message contract for the "payments" topic. Published once per POST /payments call
 * and consumed by PaymentConsumer, which drives PaymentService.process().
 */
public record PaymentEvent(
        String paymentId,
        String fromAccount,
        String toAccount,
        BigDecimal amount,
        String currency) {
}
