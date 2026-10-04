package com.example.paymentevent.integration;

import com.example.paymentevent.kafka.KafkaPublishException;
import com.example.paymentevent.kafka.PaymentEvent;
import com.example.paymentevent.kafka.PaymentFailedEvent;
import com.example.paymentevent.kafka.PaymentProducer;
import com.example.paymentevent.kafka.TransactionEvent;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Replaces PaymentProducer in every IT (imported by AbstractIntegrationTest, so all IT
 * classes still share one Spring context - a second context would run its own relay and
 * consumers against the same database and broker, and race the test). Passes through to
 * the real producer unless a test has asked for the next N publishes of a specific
 * (topic, key) to fail, which simulates a broker/network failure for just that message
 * without touching anything else running concurrently. Thread-safe: publishes come from
 * request, listener and scheduler threads.
 */
@Primary
public class FaultInjectingPaymentProducer extends PaymentProducer {

    private final Map<String, AtomicInteger> pendingFailures = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> publishCalls = new ConcurrentHashMap<>();

    public FaultInjectingPaymentProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        super(kafkaTemplate);
    }

    public void failNextPublishes(String topic, String key, int times) {
        pendingFailures.put(slot(topic, key), new AtomicInteger(times));
    }

    public int remainingFailures(String topic, String key) {
        AtomicInteger remaining = pendingFailures.get(slot(topic, key));
        return remaining == null ? 0 : remaining.get();
    }

    /** How many times a publish was attempted for (topic, key), failed or not, since the last clear(). */
    public int publishCount(String topic, String key) {
        AtomicInteger calls = publishCalls.get(slot(topic, key));
        return calls == null ? 0 : calls.get();
    }

    public void clear() {
        pendingFailures.clear();
        publishCalls.clear();
    }

    @Override
    public void publishPaymentEvent(PaymentEvent event) {
        maybeFail("payments", event.paymentId());
        super.publishPaymentEvent(event);
    }

    @Override
    public void publishTransactionEvent(TransactionEvent event) {
        maybeFail("transactions", event.paymentId());
        super.publishTransactionEvent(event);
    }

    @Override
    public void publishPaymentFailedEvent(PaymentFailedEvent event) {
        maybeFail("payment.failed", event.paymentId());
        super.publishPaymentFailedEvent(event);
    }

    private void maybeFail(String topic, String key) {
        publishCalls.computeIfAbsent(slot(topic, key), k -> new AtomicInteger()).incrementAndGet();
        AtomicInteger remaining = pendingFailures.get(slot(topic, key));
        if (remaining != null && remaining.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
            throw new KafkaPublishException(topic, key, new IllegalStateException("injected publish failure"));
        }
    }

    private static String slot(String topic, String key) {
        return topic + "|" + key;
    }
}
