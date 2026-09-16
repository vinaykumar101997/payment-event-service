package com.example.paymentevent.exception;

public class DuplicatePaymentException extends RuntimeException {

    public DuplicatePaymentException(String paymentId) {
        super("Payment already exists: " + paymentId);
    }
}
