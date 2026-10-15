package com.example.paymentevent.screening;

/** The verdict values of contracts/payment.screening.completed.v1.schema.json. */
public enum Verdict {
    /** Nothing found: the payment is released for processing. */
    CLEAR,
    /** Something needs a human look: the payment stays HELD for an officer. */
    FLAG,
    /** The payment must not go ahead: it is rejected. */
    REJECT,
    /** Screening could not decide (e.g. a data source was down): the payment stays HELD. */
    ERROR
}
