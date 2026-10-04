package com.example.paymentevent.exception;

/**
 * The amount breaks a money rule (see MoneyRules): too many decimal places for its currency,
 * an unsupported currency, or above the configured maximum. Mapped to 400 invalid-amount.
 */
public class InvalidAmountException extends InvalidPaymentRequestException {

    public InvalidAmountException(String message) {
        super(message);
    }
}
