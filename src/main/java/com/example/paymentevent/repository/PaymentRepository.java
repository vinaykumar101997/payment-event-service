package com.example.paymentevent.repository;

import com.example.paymentevent.domain.Payment;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.List;

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
     * Candidates for PaymentSweeper: still RECEIVED (never even claimed by a consumer) and
     * old enough that normal processing should have picked them up by now. The
     * lastSweptAt check gives a cooldown so a payment that's still stuck isn't republished
     * every scheduler tick, only once per threshold window.
     */
    @Query("SELECT p FROM Payment p WHERE p.status = com.example.paymentevent.domain.PaymentStatus.RECEIVED " +
           "AND p.createdAt < :threshold " +
           "AND (p.lastSweptAt IS NULL OR p.lastSweptAt < :threshold)")
    List<Payment> findStaleReceivedPayments(@Param("threshold") Instant threshold);

    /**
     * Bulk update rather than load-mutate-save, so this never risks reviving a stale
     * cached entity for a row PaymentSweeper is only bookkeeping, not processing.
     */
    @Modifying
    @Query("UPDATE Payment p SET p.lastSweptAt = :sweptAt WHERE p.paymentId = :paymentId")
    void markSwept(@Param("paymentId") String paymentId, @Param("sweptAt") Instant sweptAt);
}
