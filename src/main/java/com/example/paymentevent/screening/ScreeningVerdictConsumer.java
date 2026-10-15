package com.example.paymentevent.screening;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Reads verdicts from the screening service. Validate first, then apply: a payload that
 * fails the schema never reaches ScreeningService, and its exception sends the record to the
 * DLT unchanged (ScreeningKafkaConfig). Only active with screening on.
 */
@Component
@ConditionalOnProperty(name = "payments.screening.enabled", havingValue = "true")
public class ScreeningVerdictConsumer {

    private static final Logger log = LoggerFactory.getLogger(ScreeningVerdictConsumer.class);

    private final ScreeningContracts contracts;
    private final ScreeningService screeningService;

    public ScreeningVerdictConsumer(ScreeningContracts contracts, ScreeningService screeningService) {
        this.contracts = contracts;
        this.screeningService = screeningService;
    }

    @KafkaListener(topics = "payment.screening.completed", containerFactory = "screeningListenerContainerFactory")
    public void onVerdict(ConsumerRecord<String, String> record) {
        ScreeningCompletedEvent event;
        try {
            event = contracts.parseCompleted(record.value());
        } catch (RuntimeException ex) {
            log.warn("Invalid verdict at {}-{}@{} (key {}) goes to the DLT: {}",
                    record.topic(), record.partition(), record.offset(), record.key(), ex.getMessage());
            throw ex;
        }
        VerdictOutcome outcome = screeningService.applyVerdict(event);
        log.debug("Verdict event {} for payment {}: {}", event.eventId(), event.paymentId(), outcome);
    }
}
