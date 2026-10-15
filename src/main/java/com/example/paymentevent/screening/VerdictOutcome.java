package com.example.paymentevent.screening;

/** What applying one verdict event did. Stored in screening_verdicts.outcome, except DUPLICATE. */
public enum VerdictOutcome {
    /** CLEAR on a HELD payment: now RECEIVED, "payments" event enqueued. */
    RELEASED,
    /** REJECT on a HELD payment: now REJECTED, payment.failed enqueued. */
    REJECTED,
    /** FLAG or ERROR on a HELD payment: no change. */
    KEPT_HELD,
    /** Any verdict for a payment that had already left HELD: recorded, not applied. */
    IGNORED_NOT_HELD,
    /** The same (paymentId, eventId) was already recorded: nothing done, nothing stored. */
    DUPLICATE
}
