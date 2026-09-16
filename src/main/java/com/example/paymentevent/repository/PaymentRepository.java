package com.example.paymentevent.repository;

import com.example.paymentevent.domain.Payment;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

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
}
