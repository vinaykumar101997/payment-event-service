package com.example.paymentevent.api;

import com.example.paymentevent.domain.Payment;
import com.example.paymentevent.exception.DuplicatePaymentException;
import com.example.paymentevent.exception.ResourceNotFoundException;
import com.example.paymentevent.kafka.KafkaPublishException;
import com.example.paymentevent.kafka.PaymentEvent;
import com.example.paymentevent.kafka.PaymentProducer;
import com.example.paymentevent.repository.PaymentRepository;
import com.example.paymentevent.service.PaymentService;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.net.URI;

@RestController
@RequestMapping("/payments")
public class PaymentController {

    private static final Logger log = LoggerFactory.getLogger(PaymentController.class);

    private final PaymentService paymentService;
    private final PaymentProducer paymentProducer;
    private final PaymentRepository paymentRepository;

    public PaymentController(PaymentService paymentService, PaymentProducer paymentProducer,
                              PaymentRepository paymentRepository) {
        this.paymentService = paymentService;
        this.paymentProducer = paymentProducer;
        this.paymentRepository = paymentRepository;
    }

    @PostMapping
    public ResponseEntity<PaymentResponse> createPayment(@Valid @RequestBody PaymentRequest request) {
        if (request.fromAccount().equals(request.toAccount())) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "fromAccount and toAccount must differ");
        }

        Payment payment;
        try {
            payment = paymentService.createPayment(request.paymentId(), request.fromAccount(),
                    request.toAccount(), request.amount(), request.currency());
        } catch (DuplicatePaymentException ex) {
            Payment existing = paymentRepository.findById(request.paymentId()).orElseThrow(() -> ex);
            log.info("Ignored duplicate POST for paymentId={}", existing.getPaymentId());
            return ResponseEntity.status(HttpStatus.CONFLICT).body(PaymentResponse.from(existing));
        }

        // The row is already durably persisted at this point; a publish failure here is
        // logged, not fatal to the request - PaymentSweeper republishes it if it's still
        // RECEIVED after 60s.
        try {
            paymentProducer.publishPaymentEvent(PaymentEvent.from(payment));
        } catch (KafkaPublishException ex) {
            log.warn("Failed to publish payment event for paymentId={}; the sweeper will retry it",
                    payment.getPaymentId(), ex);
        }

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
