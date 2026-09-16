package com.example.paymentevent.exception;

public class AccountNotFoundException extends PaymentValidationException {

    public AccountNotFoundException(String accountId) {
        super("Account not found: " + accountId);
    }
}
