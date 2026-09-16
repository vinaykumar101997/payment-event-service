package com.example.paymentevent.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Declares topics explicitly rather than relying on broker auto-create, so partition
 * counts are deterministic across local, test, and CI environments.
 *
 * "payments" and "transactions" use 3 partitions to match spring.kafka.listener.concurrency=3
 * (application.yml) - correctness never depends on this, since PaymentService serializes
 * conflicting writes via row locks regardless of which partition/thread they land on, but
 * matching partition count to consumer concurrency is what lets those threads actually run
 * in parallel instead of one instance idling.
 */
@Configuration
public class KafkaTopicsConfig {

    @Bean
    public NewTopic paymentsTopic() {
        return TopicBuilder.name("payments").partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic paymentsDltTopic() {
        return TopicBuilder.name("payments.DLT").partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic transactionsTopic() {
        return TopicBuilder.name("transactions").partitions(3).replicas(1).build();
    }
}
