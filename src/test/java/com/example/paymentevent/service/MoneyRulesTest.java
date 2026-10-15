package com.example.paymentevent.service;

import com.example.paymentevent.exception.InvalidAmountException;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MoneyRulesTest {

    private final MoneyRules moneyRules = new MoneyRules(new BigDecimal("1000000.00"));

    @Test
    void usdAllowsTwoDecimalPlaces() {
        assertAccepted("10.25", "USD");
        assertAccepted("10", "USD");
    }

    @Test
    void usdRejectsThreeDecimalPlaces() {
        assertRejected("10.255", "USD", "more decimal places than USD allows (2)");
    }

    @Test
    void jpyAllowsWholeAmountsIncludingTrailingZeroDecimals() {
        assertAccepted("100", "JPY");
        assertAccepted("100.00", "JPY");
    }

    @Test
    void jpyRejectsAnyFraction() {
        assertRejected("100.5", "JPY", "more decimal places than JPY allows (0)");
        assertRejected("0.01", "JPY", "more decimal places than JPY allows (0)");
    }

    @Test
    void unknownCurrencyIsRejected() {
        assertRejected("10.00", "ABC", "Unknown currency: ABC");
    }

    /** XAU (gold) and similar ISO codes have no minor unit (default fraction digits -1). */
    @Test
    void currencyWithoutMinorUnitIsRejected() {
        assertRejected("10", "XAU", "XAU");
    }

    /** KWD has 3 decimal places; NUMERIC(19,2) would silently round 1.234 to 1.23. */
    @Test
    void currencyWithMoreDecimalPlacesThanStorageIsRejected() {
        assertRejected("1.234", "KWD", "KWD uses 3 decimal places");
        assertRejected("1.23", "KWD", "KWD uses 3 decimal places");
    }

    @Test
    void amountUpToTheMaximumIsAccepted() {
        assertAccepted("1000000.00", "USD");
        assertAccepted("1000000", "JPY");
    }

    @Test
    void amountAboveTheMaximumIsRejected() {
        assertRejected("1000000.01", "USD", "exceeds the maximum of 1000000.00");
        assertRejected("1000001", "JPY", "exceeds the maximum");
    }

    @Test
    void maximumIsConfigurable() {
        MoneyRules strict = new MoneyRules(new BigDecimal("50.00"));

        assertThatCode(() -> strict.validate(new BigDecimal("50.00"), "USD")).doesNotThrowAnyException();
        assertThatThrownBy(() -> strict.validate(new BigDecimal("50.01"), "USD"))
                .isInstanceOf(InvalidAmountException.class)
                .hasMessageContaining("exceeds the maximum of 50.00");
    }

    /**
     * The same number is a modest JPY payment and a very large USD one, so one cap can't fit
     * both: the per-currency map overrides the default for the currencies it names.
     */
    @Test
    void sameAmountIsAcceptedInOneCurrencyAndRejectedInAnother() {
        MoneyRules perCurrency = new MoneyRules(new BigDecimal("1000000.00"), Map.of("JPY", new BigDecimal("150000000")));

        assertThatCode(() -> perCurrency.validate(new BigDecimal("5000000"), "JPY")).doesNotThrowAnyException();
        assertThatThrownBy(() -> perCurrency.validate(new BigDecimal("5000000"), "USD"))
                .isInstanceOf(InvalidAmountException.class)
                .hasMessageContaining("exceeds the maximum of 1000000.00 USD");
        assertThatThrownBy(() -> perCurrency.validate(new BigDecimal("150000001"), "JPY"))
                .isInstanceOf(InvalidAmountException.class)
                .hasMessageContaining("exceeds the maximum of 150000000 JPY");
    }

    /** Map keys are matched case-insensitively (environment variables arrive lower-cased). */
    @Test
    void perCurrencyKeysAreCaseInsensitive() {
        MoneyRules perCurrency = new MoneyRules(new BigDecimal("100.00"), Map.of("jpy", new BigDecimal("10000")));

        assertThatCode(() -> perCurrency.validate(new BigDecimal("10000"), "JPY")).doesNotThrowAnyException();
    }

    /** A typo in the configuration fails at start-up instead of silently using the default. */
    @Test
    void invalidPerCurrencyConfigurationIsRejectedAtConstruction() {
        assertThatThrownBy(() -> new MoneyRules(new BigDecimal("100.00"), Map.of("XYZ", new BigDecimal("10"))))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("XYZ");
        assertThatThrownBy(() -> new MoneyRules(new BigDecimal("100.00"), Map.of("USD", BigDecimal.ZERO)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("USD");
    }

    private void assertAccepted(String amount, String currency) {
        assertThatCode(() -> moneyRules.validate(new BigDecimal(amount), currency)).doesNotThrowAnyException();
    }

    private void assertRejected(String amount, String currency, String messagePart) {
        assertThatThrownBy(() -> moneyRules.validate(new BigDecimal(amount), currency))
                .isInstanceOf(InvalidAmountException.class)
                .hasMessageContaining(messagePart);
    }
}
