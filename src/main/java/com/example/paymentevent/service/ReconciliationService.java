package com.example.paymentevent.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Checks the two double-entry invariants:
 *  1. every payment's ledger entries sum to zero (each debit has a matching credit);
 *  2. every account's balance equals its opening balance plus the sum of its entries
 *     (no balance change happened without a ledger entry, and vice versa).
 *
 * Each check is a single SQL statement, so it reads one consistent snapshot - a transfer
 * committing concurrently is either fully visible to it or not at all, never half-applied.
 */
@Service
public class ReconciliationService {

    private static final Logger log = LoggerFactory.getLogger(ReconciliationService.class);

    private final JdbcTemplate jdbcTemplate;

    public ReconciliationService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public ReconciliationReport reconcile() {
        List<ReconciliationReport.UnbalancedPayment> unbalancedPayments = jdbcTemplate.query(
                "SELECT payment_id, SUM(amount) AS entry_sum FROM ledger_entries "
                        + "GROUP BY payment_id HAVING SUM(amount) <> 0 ORDER BY payment_id",
                (rs, rowNum) -> new ReconciliationReport.UnbalancedPayment(
                        rs.getString("payment_id"), rs.getBigDecimal("entry_sum")));

        List<ReconciliationReport.AccountDiscrepancy> accountDiscrepancies = jdbcTemplate.query(
                "SELECT a.id, a.balance, a.opening_balance, COALESCE(e.entry_sum, 0) AS entry_sum "
                        + "FROM accounts a "
                        + "LEFT JOIN (SELECT account_id, SUM(amount) AS entry_sum FROM ledger_entries "
                        + "           GROUP BY account_id) e ON e.account_id = a.id "
                        + "WHERE a.balance <> a.opening_balance + COALESCE(e.entry_sum, 0) "
                        + "ORDER BY a.id",
                (rs, rowNum) -> new ReconciliationReport.AccountDiscrepancy(
                        rs.getString("id"), rs.getBigDecimal("balance"),
                        rs.getBigDecimal("opening_balance"), rs.getBigDecimal("entry_sum")));

        ReconciliationReport report = new ReconciliationReport(unbalancedPayments, accountDiscrepancies);
        if (!report.isClean()) {
            log.error("Ledger reconciliation failed: {}", report);
        }
        return report;
    }
}
