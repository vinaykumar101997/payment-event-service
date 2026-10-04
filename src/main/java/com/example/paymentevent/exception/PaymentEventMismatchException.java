package com.example.paymentevent.exception;

/**
 * A "payments" event whose fields disagree with the stored Payment row. The row is the
 * source of truth, but a disagreement means something upstream (publisher bug, tampering,
 * corruption) can't be trusted, so no money is moved at all. Unlike the other
 * PaymentValidationExceptions it is rethrown out of PaymentService.process(): it is
 * non-retryable and routed straight to the DLT for investigation.
 */
public class PaymentEventMismatchException extends PaymentValidationException {

    public PaymentEventMismatchException(String message) {
        super(message);
    }
}
