package com.example.paymentevent.kafka;

import com.example.paymentevent.domain.Payment;

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

    public static PaymentEvent from(Payment payment) {
        return new PaymentEvent(payment.getPaymentId(), payment.getFromAccount(), payment.getToAccount(),
                payment.getAmount(), payment.getCurrency());
    }
}
