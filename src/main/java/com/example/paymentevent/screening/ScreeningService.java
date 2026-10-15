package com.example.paymentevent.screening;

import com.example.paymentevent.domain.Payment;
import com.example.paymentevent.domain.PaymentStatus;
import com.example.paymentevent.exception.UnknownScreenedPaymentException;
import com.example.paymentevent.kafka.PaymentEvent;
import com.example.paymentevent.kafka.PaymentFailedEvent;
import com.example.paymentevent.outbox.OutboxWriter;
import com.example.paymentevent.repository.PaymentRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Applies screening verdicts to HELD payments, one transaction per verdict event:
 *   1. Lock the payment row, so a verdict and an officer decision for the same payment run
 *      one after the other.
 *   2. Work out the outcome from the current status: only a HELD payment can change.
 *   3. Record the event in screening_verdicts keyed (paymentId, eventId). If that key exists
 *      the event is a redelivery: nothing else happens (DUPLICATE).
 *   4. Apply it: CLEAR -> RECEIVED plus the "payments" outbox row (from here it is an ordinary
 *      payment); REJECT -> REJECTED plus payment.failed; FLAG and ERROR -> stays HELD.
 * A verdict for a payment that already left HELD (an earlier CLEAR or REJECT, or an officer)
 * is recorded as IGNORED_NOT_HELD and never applied: a decision is made once.
 */
@Service
public class ScreeningService {

    private static final Logger log = LoggerFactory.getLogger(ScreeningService.class);
    private static final int MAX_FAILURE_REASON_LENGTH = 500;

    private final PaymentRepository paymentRepository;
    private final ScreeningVerdictRepository verdictRepository;
    private final OutboxWriter outboxWriter;

    public ScreeningService(PaymentRepository paymentRepository, ScreeningVerdictRepository verdictRepository,
                            OutboxWriter outboxWriter) {
        this.paymentRepository = paymentRepository;
        this.verdictRepository = verdictRepository;
        this.outboxWriter = outboxWriter;
    }

    @Transactional
    public VerdictOutcome applyVerdict(ScreeningCompletedEvent event) {
        Payment payment = paymentRepository.findByIdForUpdate(event.paymentId())
                .orElseThrow(() -> new UnknownScreenedPaymentException(event.paymentId()));

        VerdictOutcome outcome = outcomeFor(payment.getStatus(), event.verdict());
        if (!verdictRepository.insertIfAbsent(event, outcome)) {
            verdictRepository.findVerdict(event.paymentId(), event.eventId())
                    .filter(stored -> stored != event.verdict())
                    .ifPresent(stored -> log.warn("Verdict event {} for payment {} was redelivered with verdict {} "
                                    + "but was recorded as {}; the recorded one stands",
                            event.eventId(), event.paymentId(), event.verdict(), stored));
            log.info("Duplicate verdict event {} for payment {} ignored", event.eventId(), event.paymentId());
            return VerdictOutcome.DUPLICATE;
        }

        switch (outcome) {
            case RELEASED -> {
                release(payment);
                log.info("Payment {} cleared by screening (event {}, run {})",
                        payment.getPaymentId(), event.eventId(), event.screeningRunId());
            }
            case REJECTED -> {
                reject(payment, "Rejected by screening" + (event.reasons().isEmpty() ? "" : ": "
                        + String.join("; ", event.reasons())));
                log.info("Payment {} rejected by screening (event {}, run {}): {}",
                        payment.getPaymentId(), event.eventId(), event.screeningRunId(), event.reasons());
            }
            case KEPT_HELD -> log.info("Payment {} stays HELD after screening verdict {} (event {}): {}",
                    payment.getPaymentId(), event.verdict(), event.eventId(), event.reasons());
            case IGNORED_NOT_HELD -> log.warn("Screening verdict {} (event {}) for payment {} ignored: "
                            + "the payment is {} and was already decided",
                    event.verdict(), event.eventId(), payment.getPaymentId(), payment.getStatus());
            default -> throw new IllegalStateException("Unexpected outcome " + outcome);
        }
        return outcome;
    }

    /** HELD -> RECEIVED and the "payments" event, in the caller's transaction. */
    void release(Payment payment) {
        payment.releaseFromHold();
        outboxWriter.enqueuePaymentEvent(PaymentEvent.from(payment));
    }

    /** HELD -> REJECTED and payment.failed, in the caller's transaction. */
    void reject(Payment payment, String reason) {
        payment.reject(reason.length() > MAX_FAILURE_REASON_LENGTH ? reason.substring(0, MAX_FAILURE_REASON_LENGTH) : reason);
        outboxWriter.enqueuePaymentFailedEvent(PaymentFailedEvent.from(payment));
    }

    private static VerdictOutcome outcomeFor(PaymentStatus status, Verdict verdict) {
        if (status != PaymentStatus.HELD) {
            return VerdictOutcome.IGNORED_NOT_HELD;
        }
        return switch (verdict) {
            case CLEAR -> VerdictOutcome.RELEASED;
            case REJECT -> VerdictOutcome.REJECTED;
            case FLAG, ERROR -> VerdictOutcome.KEPT_HELD;
        };
    }
}
