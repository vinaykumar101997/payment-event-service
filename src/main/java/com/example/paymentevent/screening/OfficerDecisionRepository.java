package com.example.paymentevent.screening;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

/** officer_decisions (V11): at most one decision per payment, insert-only. */
@Repository
public class OfficerDecisionRepository {

    private final JdbcTemplate jdbcTemplate;

    public OfficerDecisionRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void insert(String paymentId, OfficerDecision decision, String officer, String note) {
        jdbcTemplate.update("INSERT INTO officer_decisions (payment_id, decision, officer, note) VALUES (?, ?, ?, ?)",
                paymentId, decision.name(), officer, note);
    }

    public Optional<OfficerDecision> findDecision(String paymentId) {
        List<String> decisions = jdbcTemplate.queryForList(
                "SELECT decision FROM officer_decisions WHERE payment_id = ?", String.class, paymentId);
        return decisions.stream().findFirst().map(OfficerDecision::valueOf);
    }
}
