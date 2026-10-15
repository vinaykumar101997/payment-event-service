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
    POISONED,
    /**
     * Waiting for fraud screening (payments.screening.enabled=true). Nothing processes it: it
     * has no "payments" event. Leaves only by a CLEAR verdict or an officer release (to
     * RECEIVED) or a REJECT verdict or an officer reject (to REJECTED). FLAG and ERROR
     * verdicts leave it HELD; nothing ever releases it automatically.
     */
    HELD,
    /** Terminal: rejected by screening or by an officer. No money moved. */
    REJECTED
}
