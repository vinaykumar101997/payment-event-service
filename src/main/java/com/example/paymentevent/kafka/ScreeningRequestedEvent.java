package com.example.paymentevent.kafka;

import com.example.paymentevent.domain.Account;
import com.example.paymentevent.domain.Payment;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.format.DateTimeFormatter;
import java.util.UUID;

/**
 * Message contract for the "payment.screening.requested" topic, defined by
 * contracts/payment.screening.requested.v1.schema.json. amount and createdAt are strings on
 * purpose: the schema requires a decimal string and an RFC 3339 timestamp, and building them
 * here keeps the wire format independent of how any ObjectMapper is configured.
 *
 * The holder name and country of each side are optional. NON_NULL leaves an unknown one out
 * of the JSON, because the schema allows a missing field but not a null one.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ScreeningRequestedEvent(
        int schemaVersion,
        String eventId,
        String paymentId,
        String fromAccount,
        String toAccount,
        String amount,
        String currency,
        String createdAt,
        String fromAccountHolderName,
        String fromAccountCountry,
        String toAccountHolderName,
        String toAccountCountry) {

    public static final int SCHEMA_VERSION = 1;

    /** from and to may be null (account not found): the event then carries no details for that side. */
    public static ScreeningRequestedEvent from(Payment payment, Account from, Account to) {
        return new ScreeningRequestedEvent(SCHEMA_VERSION, UUID.randomUUID().toString(), payment.getPaymentId(),
                payment.getFromAccount(), payment.getToAccount(), payment.getAmount().toPlainString(),
                payment.getCurrency(), DateTimeFormatter.ISO_INSTANT.format(payment.getCreatedAt()),
                from == null ? null : from.getHolderName(), from == null ? null : from.getCountry(),
                to == null ? null : to.getHolderName(), to == null ? null : to.getCountry());
    }
}
