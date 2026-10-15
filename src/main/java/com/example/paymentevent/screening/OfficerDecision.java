package com.example.paymentevent.screening;

/** What a compliance officer decided about a HELD payment (officer_decisions.decision). */
public enum OfficerDecision {
    /** Like a CLEAR verdict: HELD -> RECEIVED, processed normally from there. */
    RELEASE,
    /** Like a REJECT verdict: HELD -> REJECTED, payment.failed published. */
    REJECT
}
