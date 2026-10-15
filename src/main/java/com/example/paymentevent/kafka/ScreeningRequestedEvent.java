package com.example.paymentevent.kafka;

import com.example.paymentevent.domain.Payment;

import java.time.format.DateTimeFormatter;
import java.util.UUID;

/**
 * Message contract for the "payment.screening.requested" topic, defined by
 * contracts/payment.screening.requested.v1.schema.json. amount and createdAt are strings on
 * purpose: the schema requires a decimal string and an RFC 3339 timestamp, and building them
 * here keeps the wire format independent of how any ObjectMapper is configured.
 */
public record ScreeningRequestedEvent(
        int schemaVersion,
        String eventId,
        String paymentId,
        String fromAccount,
        String toAccount,
        String amount,
        String currency,
        String createdAt) {

    public static final int SCHEMA_VERSION = 1;

    public static ScreeningRequestedEvent from(Payment payment) {
        return new ScreeningRequestedEvent(SCHEMA_VERSION, UUID.randomUUID().toString(), payment.getPaymentId(),
                payment.getFromAccount(), payment.getToAccount(), payment.getAmount().toPlainString(),
                payment.getCurrency(), DateTimeFormatter.ISO_INSTANT.format(payment.getCreatedAt()));
    }
}
