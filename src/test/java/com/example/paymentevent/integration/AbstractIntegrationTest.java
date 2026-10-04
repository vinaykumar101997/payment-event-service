package com.example.paymentevent.integration;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

/**
 * Deliberately NOT using @Testcontainers/@Container here. That JUnit5 extension stops a
 * "static" container in afterAll() of whichever test class happens to run - which is
 * exactly wrong for a container meant to be shared across every class extending this
 * base: the first class to finish kills it, and every later class then binds a cached
 * Spring context to a now-dead port. Starting the containers once in a static
 * initializer instead - and never calling stop() - is the documented pattern for sharing
 * containers across test classes; Testcontainers' Ryuk reaper cleans them up on JVM exit.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(FaultInjectingPaymentProducer.class)
public abstract class AbstractIntegrationTest {

    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"))
                    .withDatabaseName("payments")
                    .withUsername("payments")
                    .withPassword("payments");

    static final ConfluentKafkaContainer KAFKA =
            new ConfluentKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.7.1"));

    static {
        POSTGRES.start();
        KAFKA.start();
    }

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        // Production caps the outbox retry backoff at 30s; tests that inject repeated publish
        // failures would otherwise spend most of their time waiting out the backoff.
        registry.add("outbox.relay.max-backoff-ms", () -> "2000");
        // Small attempt limits so poison-payment / dead-outbox-row tests finish in seconds.
        // The outbox limit must stay above the 6 attempts OutboxIT's recovery test needs.
        registry.add("payments.processing.max-attempts", () -> "3");
        registry.add("outbox.relay.max-attempts", () -> "8");
        // Tests call PaymentSweeper.sweep() directly; a background sweep firing mid-test would
        // claim their stale payments first and make the outcome depend on timing.
        registry.add("payments.sweeper.interval-ms", () -> "3600000");
    }

    /**
     * Scans the whole topic from the beginning for a record with the given key, since every
     * IT class publishes to the same shared topics. Returns false if none shows up in time.
     */
    protected static boolean observeRecordKey(String topic, String expectedKey, Duration timeout) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-" + topic + "-reader-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            long deadline = System.currentTimeMillis() + timeout.toMillis();
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    if (expectedKey.equals(record.key())) {
                        return true;
                    }
                }
            }
            return false;
        }
    }
}
