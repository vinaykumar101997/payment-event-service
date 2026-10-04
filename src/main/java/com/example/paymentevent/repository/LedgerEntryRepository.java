package com.example.paymentevent.repository;

import com.example.paymentevent.domain.LedgerEntry;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface LedgerEntryRepository extends JpaRepository<LedgerEntry, Long> {

    List<LedgerEntry> findByPaymentId(String paymentId);
}
