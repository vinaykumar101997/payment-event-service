package com.example.paymentevent.screening;

import java.time.Duration;

/**
 * Called once per payment when HeldPaymentSweeper marks it overdue (HELD longer than
 * payments.screening.timeout). This is where a counter or an alert plugs in: implement it as
 * a bean and the sweeper calls it. PaymentMetrics is one: it counts them as
 * payments.screening.overdue, next to the sweeper's WARN log line.
 *
 * Called after the overdue mark has committed, on the scheduler thread. An exception is
 * logged and does not stop other hooks or other payments.
 */
public interface HeldPaymentOverdueHook {

    void onOverdue(String paymentId, Duration heldFor);
}
