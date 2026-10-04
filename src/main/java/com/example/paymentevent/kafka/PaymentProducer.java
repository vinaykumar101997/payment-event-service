package com.example.paymentevent.kafka;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Both methods block until the broker acknowledges (or a bounded timeout elapses) and
 * throw KafkaPublishException on failure, rather than firing-and-forgetting. The main caller
 * is OutboxRelay, which marks an outbox row published only once this returns - a
 * fire-and-forget send would let it mark rows published that the broker never received.
 */
@Component
public class PaymentProducer {

    private static final long SEND_TIMEOUT_SECONDS = 5;

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public PaymentProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishPaymentEvent(PaymentEvent event) {
        send("payments", event.paymentId(), event);
    }

    public void publishTransactionEvent(TransactionEvent event) {
        send("transactions", event.paymentId(), event);
    }

    public void publishPaymentFailedEvent(PaymentFailedEvent event) {
        send("payment.failed", event.paymentId(), event);
    }

    private void send(String topic, String key, Object value) {
        try {
            kafkaTemplate.send(topic, key, value).get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaPublishException(topic, key, e);
        } catch (ExecutionException | TimeoutException e) {
            throw new KafkaPublishException(topic, key, e);
        }
    }
}
