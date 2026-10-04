package com.example.paymentevent.repository;

import com.example.paymentevent.domain.Payment;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;

public interface PaymentRepository extends JpaRepository<Payment, String> {

    /**
     * Atomically claims a RECEIVED payment for processing by flipping it to PROCESSING.
     * Returns 1 if this call won the claim, 0 if the payment doesn't exist in RECEIVED
     * state anymore (already processed/failed, or a concurrent claim beat this one).
     * clearAutomatically=true so the immediate findById() that follows a successful claim
     * doesn't read a stale cached entity from before the bulk update.
     */
    @Modifying(clearAutomatically = true)
    @Query("UPDATE Payment p SET p.status = com.example.paymentevent.domain.PaymentStatus.PROCESSING " +
           "WHERE p.paymentId = :paymentId AND p.status = com.example.paymentevent.domain.PaymentStatus.RECEIVED")
    int claimForProcessing(@Param("paymentId") String paymentId);

    /**
     * Counts one failed non-business processing attempt. Only while RECEIVED (the failed
     * attempt's own claim has rolled back by then): a payment that reached a terminal status
     * in the meantime isn't counted. A single UPDATE, so concurrent failures never lose a count.
     */
    @Modifying
    @Query("UPDATE Payment p SET p.processingAttempts = p.processingAttempts + 1 " +
           "WHERE p.paymentId = :paymentId AND p.status = com.example.paymentevent.domain.PaymentStatus.RECEIVED")
    int incrementProcessingAttempts(@Param("paymentId") String paymentId);

    /** Moves a RECEIVED payment that has used up its attempts to POISONED; returns 1 if it did. */
    @Modifying
    @Query("UPDATE Payment p SET p.status = com.example.paymentevent.domain.PaymentStatus.POISONED, " +
           "p.failureReason = :reason, p.processedAt = :now " +
           "WHERE p.paymentId = :paymentId AND p.status = com.example.paymentevent.domain.PaymentStatus.RECEIVED " +
           "AND p.processingAttempts >= :maxAttempts")
    int markPoisonedIfExhausted(@Param("paymentId") String paymentId, @Param("maxAttempts") int maxAttempts,
                                @Param("reason") String reason, @Param("now") Instant now);
}
