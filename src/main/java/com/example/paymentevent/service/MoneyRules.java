package com.example.paymentevent.service;

import com.example.paymentevent.exception.InvalidAmountException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.Currency;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Money rules applied to every new payment, before anything is persisted:
 *  - the currency must be a real ISO 4217 code with a minor unit (java.util.Currency);
 *  - the amount may not have more decimal places than the currency's minor unit (USD 2,
 *    JPY 0). Trailing zeros don't count: 100.00 JPY is the same money as 100 JPY;
 *  - currencies with more than 2 decimal places (KWD, BHD, ...) are rejected outright, since
 *    amounts and balances are stored as NUMERIC(19, 2) and Postgres would silently round them;
 *  - the amount may not exceed the currency's maximum: payments.max-amount-by-currency.<CODE>
 *    if set, otherwise the payments.max-amount default. One number can't fit every currency
 *    (5,000,000 is a modest JPY amount and a very large USD one). Map keys are matched
 *    case-insensitively; an unknown code or a non-positive value fails start-up.
 * PaymentRequest's @Digits/@Positive still do the structural checks first.
 */
@Component
public class MoneyRules {

    private static final int STORAGE_SCALE = 2;

    private final BigDecimal defaultMaxAmount;
    private final Map<String, BigDecimal> maxAmountByCurrency;

    @Autowired
    public MoneyRules(@Value("${payments.max-amount:1000000.00}") BigDecimal defaultMaxAmount,
                      MoneyLimitsProperties limits) {
        this(defaultMaxAmount, limits.maxAmountByCurrency());
    }

    public MoneyRules(BigDecimal defaultMaxAmount) {
        this(defaultMaxAmount, Map.of());
    }

    public MoneyRules(BigDecimal defaultMaxAmount, Map<String, BigDecimal> maxAmountByCurrency) {
        this.defaultMaxAmount = requirePositive("payments.max-amount", defaultMaxAmount);
        Map<String, BigDecimal> byCurrency = new HashMap<>();
        maxAmountByCurrency.forEach((code, max) -> {
            String normalized = code.toUpperCase(Locale.ROOT);
            try {
                Currency.getInstance(normalized);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("payments.max-amount-by-currency has an unknown currency: " + code);
            }
            byCurrency.put(normalized, requirePositive("payments.max-amount-by-currency." + code, max));
        });
        this.maxAmountByCurrency = Map.copyOf(byCurrency);
    }

    private static BigDecimal requirePositive(String property, BigDecimal value) {
        if (value == null || value.signum() <= 0) {
            throw new IllegalArgumentException(property + " must be a positive amount, was " + value);
        }
        return value;
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
        BigDecimal maxAmount = maxAmountByCurrency.getOrDefault(currencyCode, defaultMaxAmount);
        if (amount.compareTo(maxAmount) > 0) {
            throw new InvalidAmountException("Amount " + amount.toPlainString() + " exceeds the maximum of "
                    + maxAmount.toPlainString() + " " + currencyCode);
        }
    }
}
