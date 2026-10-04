package com.example.paymentevent.service;

/**
 * Outcome of PaymentService.process(). Informational only: the "transactions" event for a
 * processed payment is written to the outbox inside process()'s own transaction, so the
 * listener has nothing left to publish in any outcome. ALREADY_PROCESSED (a redelivery of
 * a payment that already committed) therefore needs no special handling any more - its
 * event was enqueued by the original processing transaction.
 */
public record PaymentProcessingResult(Outcome outcome) {

    public enum Outcome {
        PROCESSED_NOW,
        /** A business rule rejected it (funds, currency, account): marked FAILED, payment.failed enqueued. */
        FAILED,
        ALREADY_PROCESSED,
        SKIPPED
    }

    public static PaymentProcessingResult processedNow() {
        return new PaymentProcessingResult(Outcome.PROCESSED_NOW);
    }

    public static PaymentProcessingResult failed() {
        return new PaymentProcessingResult(Outcome.FAILED);
    }

    public static PaymentProcessingResult alreadyProcessed() {
        return new PaymentProcessingResult(Outcome.ALREADY_PROCESSED);
    }

    public static PaymentProcessingResult skipped() {
        return new PaymentProcessingResult(Outcome.SKIPPED);
    }
}
