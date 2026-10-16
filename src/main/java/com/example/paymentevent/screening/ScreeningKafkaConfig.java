package com.example.paymentevent.screening;

import com.example.paymentevent.exception.InvalidScreeningEventException;
import com.example.paymentevent.exception.UnknownScreenedPaymentException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

import java.util.HashMap;

/**
 * Listener wiring for payment.screening.completed, separate from the "payments" listener:
 *  - Values are read as plain strings. The screening service sends JSON without Spring's type
 *    headers, and the payload must be schema-validated as written (ScreeningContracts), not
 *    after a lenient mapping into a Java type.
 *  - Its own error handler: InvalidScreeningEventException and UnknownScreenedPaymentException
 *    are non-retryable and go straight to payment.screening.completed.DLT; anything else
 *    (a database blip) gets the same 1s/2x/10s backoff as the payments listener first. The DLT
 *    is written with a String serializer, so the dead letter holds the original bytes, not a
 *    JSON-quoted copy.
 * The producer and error handler are deliberately not beans: a second KafkaTemplate bean
 * would switch off Boot's auto-configured one, and a second CommonErrorHandler bean would
 * leave the payments listener without one (Boot only applies a unique handler).
 */
@Configuration
@ConditionalOnProperty(name = "payments.screening.enabled", havingValue = "true")
public class ScreeningKafkaConfig implements DisposableBean {

    private DefaultKafkaProducerFactory<String, String> dltProducerFactory;

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> screeningListenerContainerFactory(
            ConsumerFactory<?, ?> consumerFactory, ProducerFactory<?, ?> producerFactory,
            @Value("${spring.kafka.listener.concurrency:1}") int concurrency) {
        DefaultKafkaConsumerFactory<String, String> stringConsumerFactory = new DefaultKafkaConsumerFactory<>(
                new HashMap<>(consumerFactory.getConfigurationProperties()), new StringDeserializer(), new StringDeserializer());

        dltProducerFactory = new DefaultKafkaProducerFactory<>(
                new HashMap<>(producerFactory.getConfigurationProperties()), new StringSerializer(), new StringSerializer());
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(new KafkaTemplate<>(dltProducerFactory),
                (ConsumerRecord<?, ?> record, Exception ex) -> new TopicPartition(record.topic() + ".DLT", -1));

        ExponentialBackOff backOff = new ExponentialBackOff(1_000L, 2.0);
        backOff.setMaxElapsedTime(10_000L);
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);
        errorHandler.addNotRetryableExceptions(InvalidScreeningEventException.class, UnknownScreenedPaymentException.class);

        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(stringConsumerFactory);
        factory.setCommonErrorHandler(errorHandler);
        factory.setConcurrency(concurrency);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        return factory;
    }

    @Override
    public void destroy() {
        if (dltProducerFactory != null) {
            dltProducerFactory.destroy();
        }
    }
}
