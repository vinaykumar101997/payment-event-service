package com.example.paymentevent.exception;

/**
 * Thrown by PaymentConsumer on the processing attempt that exhausted a payment's attempt
 * limit and moved it to POISONED. Registered as non-retryable, so the message goes straight
 * to the DLT instead of being retried again.
 */
public class PaymentPoisonedException extends RuntimeException {

    public PaymentPoisonedException(String paymentId, Throwable lastError) {
        super("Payment " + paymentId + " poisoned after repeated processing failures", lastError);
    }
}
