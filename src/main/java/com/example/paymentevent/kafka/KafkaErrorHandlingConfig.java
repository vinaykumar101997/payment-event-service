package com.example.paymentevent.kafka;

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
 * Two failure classes reach here (see PaymentService.process()):
 *  - PaymentValidationException (insufficient funds, unknown account, currency mismatch):
 *    registered as non-retryable, so it skips the backoff entirely and goes straight to
 *    the recoverer. Retrying a business-rule failure wastes time; the outcome won't change.
 *  - Anything else (DB connectivity blips, etc.): retried with exponential backoff, then
 *    recovered to the DLT if still failing.
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
        handler.addNotRetryableExceptions(PaymentValidationException.class);
        return handler;
    }
}
