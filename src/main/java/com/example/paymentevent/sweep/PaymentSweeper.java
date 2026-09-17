package com.example.paymentevent.sweep;

import com.example.paymentevent.domain.Payment;
import com.example.paymentevent.kafka.PaymentEvent;
import com.example.paymentevent.kafka.PaymentProducer;
import com.example.paymentevent.repository.PaymentRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * Stop-gap for the dual-write problem between "insert Payment row" and "publish to Kafka"
 * in PaymentController: those two steps aren't atomic, so a payment can end up durably
 * persisted as RECEIVED with no corresponding message ever reaching the "payments" topic
 * (crash between the two steps, or a publish that failed and was only logged, not retried).
 * This sweeper republishes anything still RECEIVED after STALE_THRESHOLD, on the assumption
 * that normal consumer processing is on the order of milliseconds, not minutes.
 *
 * The production-grade fix for this is a transactional outbox: write the event to an
 * outbox table in the SAME transaction as the Payment insert, then have a separate relay
 * (e.g. Debezium CDC, or a poller) publish from the outbox to Kafka and mark it sent. That
 * makes the DB write and the "intent to publish" atomic, instead of polling for the
 * absence of a side effect after the fact. Out of scope here, but worth calling out.
 */
@Component
public class PaymentSweeper {

    private static final Logger log = LoggerFactory.getLogger(PaymentSweeper.class);
    private static final Duration STALE_THRESHOLD = Duration.ofSeconds(60);

    private final PaymentRepository paymentRepository;
    private final PaymentProducer paymentProducer;

    public PaymentSweeper(PaymentRepository paymentRepository, PaymentProducer paymentProducer) {
        this.paymentRepository = paymentRepository;
        this.paymentProducer = paymentProducer;
    }

    @Scheduled(fixedDelay = 30_000)
    public void sweep() {
        Instant threshold = Instant.now().minus(STALE_THRESHOLD);
        List<Payment> stale = paymentRepository.findStaleReceivedPayments(threshold);

        for (Payment payment : stale) {
            log.warn("Sweeping stale RECEIVED payment {} (created at {}) - republishing to \"payments\"",
                    payment.getPaymentId(), payment.getCreatedAt());
            paymentRepository.markSwept(payment.getPaymentId(), Instant.now());
            paymentProducer.publishPaymentEvent(PaymentEvent.from(payment));
        }
    }
}
