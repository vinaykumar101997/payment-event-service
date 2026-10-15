package com.example.paymentevent.integration;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Same containers as every other IT, but an application context with screening switched on.
 *
 * Closed after each class (DirtiesContext): while it is open it is a second instance of the
 * service next to the shared screening-off context - same database, same consumer group, its
 * own outbox relay. The service is built to run that way, but tests that inject faults into
 * one context's producer (OutboxIT, PoisonPaymentIT) would have their rows published by the
 * other context's healthy relay if it were still running.
 */
@TestPropertySource(properties = "payments.screening.enabled=true")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public abstract class AbstractScreeningIntegrationTest extends AbstractIntegrationTest {

    /** Sends value exactly as given (plain string, no type headers), as the Python service would. */
    protected static void sendRaw(String topic, String key, String value) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            producer.send(new ProducerRecord<>(topic, key, value)).get(10, TimeUnit.SECONDS);
        }
    }

    /** First record value with the given key on the topic, read as a plain string from the beginning. */
    protected static Optional<String> recordValue(String topic, String key, Duration timeout) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-" + topic + "-value-reader-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            long deadline = System.currentTimeMillis() + timeout.toMillis();
            while (System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
                    if (key.equals(record.key())) {
                        return Optional.ofNullable(record.value());
                    }
                }
            }
            return Optional.empty();
        }
    }

    /** A payment.screening.completed v1 event, as the screening service would send it. */
    protected static String verdictJson(String eventId, String paymentId, String verdict, String... reasons) {
        StringBuilder reasonList = new StringBuilder("[");
        for (int i = 0; i < reasons.length; i++) {
            reasonList.append(i == 0 ? "" : ",").append('"').append(reasons[i]).append('"');
        }
        reasonList.append(']');
        return "{\"schemaVersion\":1,\"eventId\":\"" + eventId + "\",\"paymentId\":\"" + paymentId
                + "\",\"verdict\":\"" + verdict + "\",\"reasons\":" + reasonList
                + ",\"riskScore\":0.5,\"decidedAt\":\"" + Instant.now() + "\",\"screeningRunId\":\"run-it\"}";
    }
}
