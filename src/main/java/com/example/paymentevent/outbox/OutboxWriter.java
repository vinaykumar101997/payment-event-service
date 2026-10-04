package com.example.paymentevent.outbox;

import com.example.paymentevent.domain.OutboxEvent;
import com.example.paymentevent.kafka.PaymentEvent;
import com.example.paymentevent.kafka.PaymentFailedEvent;
import com.example.paymentevent.kafka.TransactionEvent;
import com.example.paymentevent.repository.OutboxEventRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Records "publish this event" as a row in the caller's transaction. MANDATORY rather than
 * REQUIRED: the whole point of the outbox is that the event commits or rolls back together
 * with the state change it describes, so calling this outside a transaction is a bug and
 * fails fast instead of silently committing the event on its own.
 */
@Component
public class OutboxWriter {

    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    public OutboxWriter(OutboxEventRepository outboxEventRepository, ObjectMapper objectMapper) {
        this.outboxEventRepository = outboxEventRepository;
        this.objectMapper = objectMapper;
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void enqueuePaymentEvent(PaymentEvent event) {
        enqueue("payments", event.paymentId(), event);
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void enqueueTransactionEvent(TransactionEvent event) {
        enqueue("transactions", event.paymentId(), event);
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void enqueuePaymentFailedEvent(PaymentFailedEvent event) {
        enqueue("payment.failed", event.paymentId(), event);
    }

    private void enqueue(String topic, String key, Object event) {
        try {
            outboxEventRepository.save(new OutboxEvent(topic, key, objectMapper.writeValueAsString(event)));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Could not serialize " + event.getClass().getSimpleName()
                    + " for outbox, key=" + key, e);
        }
    }
}
