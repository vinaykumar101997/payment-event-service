package com.example.paymentevent.kafka;

import com.example.paymentevent.exception.PaymentPoisonedException;
import com.example.paymentevent.exception.PaymentValidationException;
import com.example.paymentevent.service.PaymentService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Deliberately thin: PaymentService.process() owns the entire DB transaction, including
 * writing the "transactions" event to the outbox, so this listener never publishes to
 * Kafka itself - OutboxRelay does that once the transaction has committed.
 *
 * Its one job beyond delegating is counting failed attempts. A PaymentValidationException
 * is a final outcome, not a fault, and passes straight through. Anything else is a failed
 * attempt: it's recorded against the payment and rethrown so Kafka's error handler retries
 * it - unless that attempt was the last one allowed, in which case the payment is now
 * POISONED and a non-retryable PaymentPoisonedException sends the message to the DLT.
 */
@Component
public class PaymentConsumer {

    private static final Logger log = LoggerFactory.getLogger(PaymentConsumer.class);

    private final PaymentService paymentService;

    public PaymentConsumer(PaymentService paymentService) {
        this.paymentService = paymentService;
    }

    @KafkaListener(topics = "payments")
    public void onPaymentEvent(PaymentEvent event) {
        try {
            paymentService.process(event);
        } catch (PaymentValidationException ex) {
            throw ex;
        } catch (RuntimeException ex) {
            if (recordFailedAttempt(event.paymentId(), ex)) {
                throw new PaymentPoisonedException(event.paymentId(), ex);
            }
            throw ex;
        }
    }

    /**
     * Recording the attempt needs the database too, which may be exactly what's failing. If
     * it can't be recorded, the original error is what gets retried - never the bookkeeping one.
     */
    private boolean recordFailedAttempt(String paymentId, RuntimeException failure) {
        try {
            return paymentService.recordFailedAttempt(paymentId, failure);
        } catch (RuntimeException recordFailure) {
            log.warn("Could not record failed processing attempt for payment {}", paymentId, recordFailure);
            failure.addSuppressed(recordFailure);
            return false;
        }
    }
}
