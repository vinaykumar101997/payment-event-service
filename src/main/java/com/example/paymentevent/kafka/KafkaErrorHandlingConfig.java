package com.example.paymentevent.kafka;

import com.example.paymentevent.exception.PaymentPoisonedException;
import com.example.paymentevent.exception.PaymentValidationException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

/**
 * Wires together how a failure in PaymentConsumer is handled. Spring Boot's Kafka
 * autoconfiguration picks up this DefaultErrorHandler bean automatically and applies it
 * to the listener container factory - no further wiring needed.
 *
 * Business-rule failures (insufficient funds, unknown account, currency mismatch) never get
 * here: PaymentService.process() marks the payment FAILED, publishes payment.failed via the
 * outbox, and returns normally. The DLT is only for real faults and poison messages:
 *  - PaymentEventMismatchException (a PaymentValidationException: the event disagrees with
 *    the stored row): registered as non-retryable, so it skips the backoff entirely and goes
 *    straight to the recoverer. Retrying won't make the event trustworthy.
 *  - Anything else (DB connectivity blips, etc.): retried with exponential backoff, then
 *    recovered to the DLT if still failing. Each such failure also counts toward the
 *    payment's attempt limit (PaymentConsumer); the attempt that exhausts it throws
 *    PaymentPoisonedException, which is non-retryable and goes straight to the DLT.
 *  - A malformed/undeserializable message (poison pill) is also handled here: it arrives
 *    wrapped as a DeserializationException by ErrorHandlingDeserializer (application.yml),
 *    which Spring Kafka treats as inherently non-retryable for the same reason as above.
 */
@Configuration
public class KafkaErrorHandlingConfig {

    @Bean
    public DefaultErrorHandler kafkaErrorHandler(KafkaOperations<Object, Object> kafkaOperations) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaOperations,
                (ConsumerRecord<?, ?> record, Exception ex) -> new TopicPartition(record.topic() + ".DLT", -1));

        ExponentialBackOff backOff = new ExponentialBackOff(1_000L, 2.0);
        backOff.setMaxElapsedTime(10_000L);

        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);
        handler.addNotRetryableExceptions(PaymentValidationException.class, PaymentPoisonedException.class);
        return handler;
    }
}
