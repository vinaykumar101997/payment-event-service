package com.example.paymentevent.service;

import com.example.paymentevent.kafka.TransactionEvent;

/**
 * Outcome of PaymentService.process(), reported back to the Kafka listener so it knows
 * whether to publish to "transactions" once this method's transaction has committed.
 * PROCESSED_NOW and ALREADY_PROCESSED both publish (ALREADY_PROCESSED handles the case
 * where a prior attempt committed the debit/credit but the listener died before
 * publishing — redelivery lets us safely retry just the publish step).
 */
public record PaymentProcessingResult(Outcome outcome, TransactionEvent transactionEvent) {

    public enum Outcome {
        PROCESSED_NOW,
        ALREADY_PROCESSED,
        SKIPPED
    }

    public static PaymentProcessingResult processedNow(TransactionEvent event) {
        return new PaymentProcessingResult(Outcome.PROCESSED_NOW, event);
    }

    public static PaymentProcessingResult alreadyProcessed(TransactionEvent event) {
        return new PaymentProcessingResult(Outcome.ALREADY_PROCESSED, event);
    }

    public static PaymentProcessingResult skipped() {
        return new PaymentProcessingResult(Outcome.SKIPPED, null);
    }

    public boolean requiresTransactionPublish() {
        return outcome == Outcome.PROCESSED_NOW || outcome == Outcome.ALREADY_PROCESSED;
    }
}
