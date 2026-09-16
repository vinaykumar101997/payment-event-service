package com.example.paymentevent.exception;

public class CurrencyMismatchException extends PaymentValidationException {

    public CurrencyMismatchException(String message) {
        super(message);
    }
}
