package com.example.paymentevent.exception;

import com.example.paymentevent.domain.Payment;

import java.util.Optional;

/**
 * A POST /payments reused an existing paymentId. Thrown by PaymentService.createPayment()
 * with just the id; PaymentController rethrows it carrying the existing payment once it has
 * confirmed the request is an identical replay, so the 409 response can include it.
 */
public class DuplicatePaymentException extends RuntimeException {

    private final transient Payment existing;

    public DuplicatePaymentException(String paymentId) {
        super("Payment already exists: " + paymentId);
        this.existing = null;
    }

    public DuplicatePaymentException(Payment existing) {
        super("Payment already exists: " + existing.getPaymentId());
        this.existing = existing;
    }

    public Optional<Payment> existing() {
        return Optional.ofNullable(existing);
    }
}
