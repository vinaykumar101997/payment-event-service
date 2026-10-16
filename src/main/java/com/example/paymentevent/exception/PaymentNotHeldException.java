package com.example.paymentevent.exception;

import com.example.paymentevent.domain.Payment;

/**
 * An officer release or reject for a payment that is not HELD (already released or rejected,
 * by a verdict or by the opposite officer decision, or never held). Answered 409 with the
 * payment's current state.
 */
public class PaymentNotHeldException extends RuntimeException {

    private final transient Payment payment;

    public PaymentNotHeldException(Payment payment, String action) {
        super("Cannot " + action + " payment " + payment.getPaymentId() + ": it is " + payment.getStatus()
                + ", not HELD");
        this.payment = payment;
    }

    public Payment payment() {
        return payment;
    }
}
