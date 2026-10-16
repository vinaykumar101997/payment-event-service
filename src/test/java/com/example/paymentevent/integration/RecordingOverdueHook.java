package com.example.paymentevent.integration;

import com.example.paymentevent.screening.HeldPaymentOverdueHook;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Records every overdue notification HeldPaymentSweeper sends, so HeldPaymentSweeperIT can
 * check the hook fires once per payment. Imported by AbstractIntegrationTest so every IT
 * shares one context.
 */
public class RecordingOverdueHook implements HeldPaymentOverdueHook {

    private final Map<String, Integer> calls = new ConcurrentHashMap<>();

    @Override
    public void onOverdue(String paymentId, Duration heldFor) {
        calls.merge(paymentId, 1, Integer::sum);
    }

    public int callsFor(String paymentId) {
        return calls.getOrDefault(paymentId, 0);
    }
}
