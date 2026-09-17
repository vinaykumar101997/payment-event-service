package com.example.paymentevent.api;

import com.example.paymentevent.domain.Payment;
import com.example.paymentevent.domain.PaymentStatus;

import java.math.BigDecimal;
import java.time.Instant;

public record PaymentResponse(
        String paymentId,
        String fromAccount,
        String toAccount,
        BigDecimal amount,
        String currency,
        PaymentStatus status,
        String failureReason,
        Instant createdAt,
        Instant processedAt) {

    public static PaymentResponse from(Payment payment) {
        return new PaymentResponse(
                payment.getPaymentId(),
                payment.getFromAccount(),
                payment.getToAccount(),
                payment.getAmount(),
                payment.getCurrency(),
                payment.getStatus(),
                payment.getFailureReason(),
                payment.getCreatedAt(),
                payment.getProcessedAt());
    }
}
