package com.example.paymentevent.domain;

import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

class PaymentTest {

    private static final String HASH = Payment.requestHashOf("p1", "acc-a", "acc-b", new BigDecimal("100.00"), "USD");

    @Test
    void requestHashIsStableAndIgnoresAmountScale() {
        assertThat(HASH).hasSize(64).isEqualTo(
                Payment.requestHashOf("p1", "acc-a", "acc-b", new BigDecimal("100.00"), "USD"));
        assertThat(Payment.requestHashOf("p1", "acc-a", "acc-b", new BigDecimal("100"), "USD")).isEqualTo(HASH);
        assertThat(Payment.requestHashOf("p1", "acc-a", "acc-b", new BigDecimal("100.0"), "USD")).isEqualTo(HASH);
    }

    @Test
    void requestHashChangesWhenAnyFieldChanges() {
        assertThat(Payment.requestHashOf("p2", "acc-a", "acc-b", new BigDecimal("100.00"), "USD")).isNotEqualTo(HASH);
        assertThat(Payment.requestHashOf("p1", "acc-x", "acc-b", new BigDecimal("100.00"), "USD")).isNotEqualTo(HASH);
        assertThat(Payment.requestHashOf("p1", "acc-a", "acc-x", new BigDecimal("100.00"), "USD")).isNotEqualTo(HASH);
        assertThat(Payment.requestHashOf("p1", "acc-a", "acc-b", new BigDecimal("100.01"), "USD")).isNotEqualTo(HASH);
        assertThat(Payment.requestHashOf("p1", "acc-a", "acc-b", new BigDecimal("100.00"), "EUR")).isNotEqualTo(HASH);
    }

    /** Field boundaries matter: moving a character from one field to the next is a different request. */
    @Test
    void requestHashDistinguishesFieldBoundaries() {
        assertThat(Payment.requestHashOf("p1", "acc-ab", "c", new BigDecimal("1"), "USD"))
                .isNotEqualTo(Payment.requestHashOf("p1", "acc-a", "bc", new BigDecimal("1"), "USD"));
    }

    @Test
    void newPaymentStoresItsRequestHash() {
        Payment payment = new Payment("p1", "acc-a", "acc-b", new BigDecimal("100.00"), "USD");

        assertThat(payment.getRequestHash()).isEqualTo(HASH);
        assertThat(payment.matchesRequestHash(HASH)).isTrue();
        assertThat(payment.matchesRequestHash(
                Payment.requestHashOf("p1", "acc-a", "acc-b", new BigDecimal("5.00"), "USD"))).isFalse();
    }

    /** A row from before V7 has no stored hash; it is compared on its stored columns instead. */
    @Test
    void legacyPaymentWithoutStoredHashIsComparedOnItsColumns() {
        Payment legacy = new Payment("p1", "acc-a", "acc-b", new BigDecimal("100.00"), "USD");
        ReflectionTestUtils.setField(legacy, "requestHash", null);

        assertThat(legacy.matchesRequestHash(HASH)).isTrue();
        assertThat(legacy.matchesRequestHash(
                Payment.requestHashOf("p1", "acc-a", "acc-b", new BigDecimal("5.00"), "USD"))).isFalse();
    }
}
