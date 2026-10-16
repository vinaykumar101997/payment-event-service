package com.example.paymentevent.screening;

import java.time.Instant;
import java.util.List;

/**
 * A verdict from the screening service, read from "payment.screening.completed". Only built
 * from a payload that passed contracts/payment.screening.completed.v1.schema.json (see
 * ScreeningContracts), so every field but riskScore is present.
 */
public record ScreeningCompletedEvent(
        int schemaVersion,
        String eventId,
        String paymentId,
        Verdict verdict,
        List<String> reasons,
        Double riskScore,
        Instant decidedAt,
        String screeningRunId) {
}
