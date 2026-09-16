package com.example.paymentevent.kafka;

import com.example.paymentevent.service.PaymentProcessingResult;
import com.example.paymentevent.service.PaymentService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Deliberately not @Transactional: PaymentService.process() owns the entire DB
 * transaction internally and returns before this method ever touches Kafka again. That
 * keeps the "transactions" publish outside of any transaction and after it has committed,
 * so a slow/unavailable broker can never hold a DB row lock open.
 */
@Component
public class PaymentConsumer {

    private final PaymentService paymentService;
    private final PaymentProducer paymentProducer;

    public PaymentConsumer(PaymentService paymentService, PaymentProducer paymentProducer) {
        this.paymentService = paymentService;
        this.paymentProducer = paymentProducer;
    }

    @KafkaListener(topics = "payments")
    public void onPaymentEvent(PaymentEvent event) {
        PaymentProcessingResult result = paymentService.process(event);

        if (result.requiresTransactionPublish()) {
            paymentProducer.publishTransactionEvent(result.transactionEvent());
        }
    }
}
