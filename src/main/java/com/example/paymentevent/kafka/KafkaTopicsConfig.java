package com.example.paymentevent.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
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

    @Bean
    public NewTopic paymentFailedTopic() {
        return TopicBuilder.name("payment.failed").partitions(3).replicas(1).build();
    }

    /**
     * Screening topics (contracts/README.md) are declared only when screening is on, so a
     * deployment that doesn't use it gets no extra topics on its broker.
     */
    @Bean
    @ConditionalOnProperty(name = "payments.screening.enabled", havingValue = "true")
    public NewTopic screeningRequestedTopic() {
        return TopicBuilder.name("payment.screening.requested").partitions(3).replicas(1).build();
    }

    @Bean
    @ConditionalOnProperty(name = "payments.screening.enabled", havingValue = "true")
    public NewTopic screeningCompletedTopic() {
        return TopicBuilder.name("payment.screening.completed").partitions(3).replicas(1).build();
    }

    @Bean
    @ConditionalOnProperty(name = "payments.screening.enabled", havingValue = "true")
    public NewTopic screeningCompletedDltTopic() {
        return TopicBuilder.name("payment.screening.completed.DLT").partitions(3).replicas(1).build();
    }
}
