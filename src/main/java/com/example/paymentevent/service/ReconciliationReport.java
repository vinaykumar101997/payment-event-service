package com.example.paymentevent.service;

import java.math.BigDecimal;
import java.util.List;

/**
 * Result of ReconciliationService.reconcile(). Clean means both double-entry invariants
 * hold; otherwise each list names exactly what is off and by how much.
 */
public record ReconciliationReport(List<UnbalancedPayment> unbalancedPayments,
                                   List<AccountDiscrepancy> accountDiscrepancies) {

    /** A payment whose ledger entries don't sum to zero. */
    public record UnbalancedPayment(String paymentId, BigDecimal entrySum) {
    }

    /** An account whose balance != openingBalance + sum(its ledger entries). */
    public record AccountDiscrepancy(String accountId, BigDecimal balance, BigDecimal openingBalance,
                                     BigDecimal entrySum) {

        public BigDecimal expectedBalance() {
            return openingBalance.add(entrySum);
        }
    }

    public boolean isClean() {
        return unbalancedPayments.isEmpty() && accountDiscrepancies.isEmpty();
    }
}
