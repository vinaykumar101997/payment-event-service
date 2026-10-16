package com.example.paymentevent.screening;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/** screening_verdicts (V9). Plain JDBC: the table is append-only and keyed for idempotency. */
@Repository
public class ScreeningVerdictRepository {

    private static final String INSERT_IF_ABSENT =
            "INSERT INTO screening_verdicts (payment_id, event_id, verdict, reasons, risk_score, decided_at,"
                    + " screening_run_id, outcome) VALUES (?, ?, ?, CAST(? AS jsonb), ?, ?, ?, ?)"
                    + " ON CONFLICT (payment_id, event_id) DO NOTHING";

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    public ScreeningVerdictRepository(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
    }

    /** Records the event with its outcome; false if (paymentId, eventId) was already recorded. */
    public boolean insertIfAbsent(ScreeningCompletedEvent event, VerdictOutcome outcome) {
        if (outcome == VerdictOutcome.DUPLICATE) {
            throw new IllegalArgumentException("DUPLICATE is never stored");
        }
        return jdbcTemplate.update(INSERT_IF_ABSENT, event.paymentId(), UUID.fromString(event.eventId()),
                event.verdict().name(), reasonsJson(event.reasons()), event.riskScore(),
                Timestamp.from(event.decidedAt()), event.screeningRunId(), outcome.name()) == 1;
    }

    /** The verdict stored for (paymentId, eventId), if any. */
    public Optional<Verdict> findVerdict(String paymentId, String eventId) {
        List<String> verdicts = jdbcTemplate.queryForList(
                "SELECT verdict FROM screening_verdicts WHERE payment_id = ? AND event_id = ?",
                String.class, paymentId, UUID.fromString(eventId));
        return verdicts.stream().findFirst().map(Verdict::valueOf);
    }

    private String reasonsJson(List<String> reasons) {
        try {
            return objectMapper.writeValueAsString(reasons == null ? List.of() : reasons);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Could not serialize verdict reasons", e);
        }
    }
}
