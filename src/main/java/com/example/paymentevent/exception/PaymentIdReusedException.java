package com.example.paymentevent.exception;

/**
 * POST /payments reused an existing paymentId with a different payload. Not a replay (that
 * gets the existing payment back with 409) - the client sent a different request under the
 * same idempotency key, so it's rejected with 422 and the original payment is untouched.
 */
public class PaymentIdReusedException extends RuntimeException {

    public PaymentIdReusedException(String paymentId) {
        super("paymentId " + paymentId + " already exists with a different payload");
    }
}
