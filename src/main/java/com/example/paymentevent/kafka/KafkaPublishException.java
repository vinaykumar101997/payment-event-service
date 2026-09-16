package com.example.paymentevent.kafka;

/**
 * Wraps a failed/timed-out Kafka publish as an unchecked exception. Deliberately not a
 * PaymentValidationException: this is a transient infra failure, not a business rule
 * violation, so Kafka's error handler treats it as retryable rather than routing straight
 * to the DLT.
 */
public class KafkaPublishException extends RuntimeException {

    public KafkaPublishException(String topic, String key, Throwable cause) {
        super("Failed to publish to topic=" + topic + " key=" + key, cause);
    }
}
