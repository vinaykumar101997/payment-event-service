package com.example.paymentevent.exception;

/**
 * A verdict for a paymentId this service has no payment for. A screening request is only
 * published after its payment row has committed, so this is never a timing issue: it is a
 * fault on the other side (or a message meant for another environment). Non-retryable, DLT.
 */
public class UnknownScreenedPaymentException extends RuntimeException {

    public UnknownScreenedPaymentException(String paymentId) {
        super("Verdict for unknown payment " + paymentId);
    }
}
