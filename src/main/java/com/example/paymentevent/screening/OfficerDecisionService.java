package com.example.paymentevent.screening;

import com.example.paymentevent.domain.Payment;
import com.example.paymentevent.domain.PaymentStatus;
import com.example.paymentevent.exception.PaymentNotHeldException;
import com.example.paymentevent.exception.ResourceNotFoundException;
import com.example.paymentevent.repository.PaymentRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Locale;

/**
 * Officer release and reject of HELD payments, one transaction each:
 *   1. Lock the payment row (the same lock a screening verdict takes, so the two never both
 *      act on the same HELD state).
 *   2. If this officer decision was already recorded for the payment, return the payment
 *      unchanged: a repeated request is answered as if it had just succeeded (idempotent),
 *      whoever sends it; the first officer and note stay recorded.
 *   3. Otherwise the payment must be HELD, or PaymentNotHeldException (409). That covers the
 *      opposite decision after an earlier one, and payments a verdict already decided.
 *   4. Record officer, note and time in officer_decisions and apply the transition exactly as
 *      the matching verdict does (ScreeningService.release / reject), outbox row included.
 */
@Service
public class OfficerDecisionService {

    private static final Logger log = LoggerFactory.getLogger(OfficerDecisionService.class);

    private final PaymentRepository paymentRepository;
    private final OfficerDecisionRepository decisionRepository;
    private final ScreeningService screeningService;

    public OfficerDecisionService(PaymentRepository paymentRepository, OfficerDecisionRepository decisionRepository,
                                  ScreeningService screeningService) {
        this.paymentRepository = paymentRepository;
        this.decisionRepository = decisionRepository;
        this.screeningService = screeningService;
    }

    @Transactional
    public Payment decide(String paymentId, OfficerDecision decision, String officer, String note) {
        Payment payment = paymentRepository.findByIdForUpdate(paymentId)
                .orElseThrow(() -> new ResourceNotFoundException("Payment not found: " + paymentId));

        if (decisionRepository.findDecision(paymentId).filter(decision::equals).isPresent()) {
            log.info("Repeated officer {} of payment {} by {} ignored; already recorded", decision, paymentId, officer);
            return payment;
        }
        if (payment.getStatus() != PaymentStatus.HELD) {
            throw new PaymentNotHeldException(payment, decision.name().toLowerCase(Locale.ROOT));
        }

        decisionRepository.insert(paymentId, decision, officer, note);
        switch (decision) {
            case RELEASE -> screeningService.release(payment);
            case REJECT -> screeningService.reject(payment, "Rejected by officer " + officer + ": " + note);
        }
        log.info("Payment {}: officer {} recorded {} ({})", paymentId, officer, decision, note);
        return payment;
    }
}
