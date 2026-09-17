package com.example.paymentevent.api;

import jakarta.validation.constraints.Digits;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;

import java.math.BigDecimal;

/**
 * Structural/format validation only - existence of accounts, currency match, and balance
 * are business rules that depend on mutable DB state and are re-checked by PaymentService
 * under lock at consumption time, not here (checking here could go stale before the
 * consumer runs).
 */
public record PaymentRequest(
        @NotBlank String paymentId,
        @NotBlank String fromAccount,
        @NotBlank String toAccount,
        @NotNull @Positive @Digits(integer = 17, fraction = 2) BigDecimal amount,
        @NotBlank @Pattern(regexp = "^[A-Z]{3}$", message = "must be a 3-letter ISO currency code") String currency) {
}
