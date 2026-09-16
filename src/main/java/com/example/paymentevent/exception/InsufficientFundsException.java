package com.example.paymentevent.exception;

public class InsufficientFundsException extends PaymentValidationException {

    public InsufficientFundsException(String message) {
        super(message);
    }
}
