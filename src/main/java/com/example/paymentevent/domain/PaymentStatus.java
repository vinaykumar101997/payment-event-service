package com.example.paymentevent.domain;

public enum PaymentStatus {
    RECEIVED,
    PROCESSING,
    PROCESSED,
    FAILED,
    /**
     * Terminal: processing kept failing with non-business errors (not insufficient funds etc.)
     * until the attempt limit was reached. No money moved; needs investigation.
     */
    POISONED
}
