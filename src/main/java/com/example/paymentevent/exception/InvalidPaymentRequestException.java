package com.example.paymentevent.exception;

/**
 * A payment request that is well-formed and passes field validation but is still invalid as
 * a whole (e.g. fromAccount == toAccount). Mapped to 400 invalid-request.
 */
public class InvalidPaymentRequestException extends RuntimeException {

    public InvalidPaymentRequestException(String message) {
        super(message);
    }
}
