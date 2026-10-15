package com.example.paymentevent.service;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.math.BigDecimal;
import java.util.Map;

/**
 * payments.max-amount-by-currency: per-currency maximum amount, keyed by ISO 4217 code
 * (e.g. JPY: 150000000). Currencies not listed use payments.max-amount (see MoneyRules).
 */
@ConfigurationProperties(prefix = "payments")
public record MoneyLimitsProperties(Map<String, BigDecimal> maxAmountByCurrency) {

    public MoneyLimitsProperties {
        maxAmountByCurrency = maxAmountByCurrency == null ? Map.of() : Map.copyOf(maxAmountByCurrency);
    }
}
