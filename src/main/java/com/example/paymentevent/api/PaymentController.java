package com.example.paymentevent.api;

import com.example.paymentevent.domain.Payment;
import com.example.paymentevent.exception.DuplicatePaymentException;
import com.example.paymentevent.exception.InvalidPaymentRequestException;
import com.example.paymentevent.exception.PaymentIdReusedException;
import com.example.paymentevent.exception.ResourceNotFoundException;
import com.example.paymentevent.repository.PaymentRepository;
import com.example.paymentevent.service.PaymentService;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;

@RestController
@RequestMapping("/payments")
public class PaymentController {

    private static final Logger log = LoggerFactory.getLogger(PaymentController.class);

    private final PaymentService paymentService;
    private final PaymentRepository paymentRepository;

    public PaymentController(PaymentService paymentService, PaymentRepository paymentRepository) {
        this.paymentService = paymentService;
        this.paymentRepository = paymentRepository;
    }

    @PostMapping
    public ResponseEntity<PaymentResponse> createPayment(@Valid @RequestBody PaymentRequest request) {
        if (request.fromAccount().equals(request.toAccount())) {
            throw new InvalidPaymentRequestException("fromAccount and toAccount must differ");
        }

        Payment payment;
        try {
            payment = paymentService.createPayment(request.paymentId(), request.fromAccount(),
                    request.toAccount(), request.amount(), request.currency());
        } catch (DuplicatePaymentException ex) {
            // Idempotency contract: an identical retry (same payload hash) is answered 409 with
            // the existing payment (a duplicate-payment problem carrying it in its "payment"
            // member); the same paymentId with a different payload is a 422.
            Payment existing = paymentRepository.findById(request.paymentId()).orElseThrow(() -> ex);
            String requestHash = Payment.requestHashOf(request.paymentId(), request.fromAccount(),
                    request.toAccount(), request.amount(), request.currency());
            if (!existing.matchesRequestHash(requestHash)) {
                log.warn("Rejected POST reusing paymentId={} with a different payload", existing.getPaymentId());
                throw new PaymentIdReusedException(existing.getPaymentId());
            }
            log.info("Ignored duplicate POST for paymentId={}", existing.getPaymentId());
            throw new DuplicatePaymentException(existing);
        }

        // No Kafka publish here: createPayment() wrote the "payments" event to the outbox in
        // the same transaction as the row, and OutboxRelay publishes it from there.
        return ResponseEntity.created(URI.create("/payments/" + payment.getPaymentId()))
                .body(PaymentResponse.from(payment));
    }

    @GetMapping("/{id}")
    public PaymentResponse getPayment(@PathVariable String id) {
        Payment payment = paymentRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Payment not found: " + id));
        return PaymentResponse.from(payment);
    }
}
