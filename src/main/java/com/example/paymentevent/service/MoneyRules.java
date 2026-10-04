package com.example.paymentevent.service;

import com.example.paymentevent.exception.InvalidAmountException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.Currency;

/**
 * Money rules applied to every new payment, before anything is persisted:
 *  - the currency must be a real ISO 4217 code with a minor unit (java.util.Currency);
 *  - the amount may not have more decimal places than the currency's minor unit (USD 2,
 *    JPY 0). Trailing zeros don't count: 100.00 JPY is the same money as 100 JPY;
 *  - currencies with more than 2 decimal places (KWD, BHD, ...) are rejected outright, since
 *    amounts and balances are stored as NUMERIC(19, 2) and Postgres would silently round them;
 *  - the amount may not exceed payments.max-amount. One limit for every currency - a
 *    per-currency limit would be the next step if JPY-scale amounts matter.
 * PaymentRequest's @Digits/@Positive still do the structural checks first.
 */
@Component
public class MoneyRules {

    private static final int STORAGE_SCALE = 2;

    private final BigDecimal maxAmount;

    public MoneyRules(@Value("${payments.max-amount:1000000.00}") BigDecimal maxAmount) {
        this.maxAmount = maxAmount;
    }

    public void validate(BigDecimal amount, String currencyCode) {
        Currency currency;
        try {
            currency = Currency.getInstance(currencyCode);
        } catch (IllegalArgumentException | NullPointerException e) {
            throw new InvalidAmountException("Unknown currency: " + currencyCode);
        }

        int fractionDigits = currency.getDefaultFractionDigits();
        if (fractionDigits < 0) {
            throw new InvalidAmountException("Currency " + currencyCode + " has no minor unit and can't be used for payments");
        }
        if (fractionDigits > STORAGE_SCALE) {
            throw new InvalidAmountException("Currency " + currencyCode + " uses " + fractionDigits
                    + " decimal places; amounts are stored with at most " + STORAGE_SCALE);
        }
        if (amount.stripTrailingZeros().scale() > fractionDigits) {
            throw new InvalidAmountException("Amount " + amount.toPlainString() + " has more decimal places than "
                    + currencyCode + " allows (" + fractionDigits + ")");
        }
        if (amount.compareTo(maxAmount) > 0) {
            throw new InvalidAmountException("Amount " + amount.toPlainString() + " exceeds the maximum of "
                    + maxAmount.toPlainString());
        }
    }
}
