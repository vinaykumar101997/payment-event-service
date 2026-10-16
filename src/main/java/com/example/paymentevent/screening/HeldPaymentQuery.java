package com.example.paymentevent.screening;

import com.example.paymentevent.exception.InvalidQueryException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/**
 * Lists HELD payments for officers (GET /payments?status=HELD), oldest first.
 *
 * Keyset ("cursor") pagination on (created_at, payment_id): each page asks for rows strictly
 * after the last row of the previous one, so a page is an index range scan on
 * idx_payments_status_created_at however deep the client pages, and payments released or
 * added between requests never make a page skip or repeat a row that stays HELD. An OFFSET
 * would do neither. payment_id breaks ties between payments held in the same microsecond.
 *
 * The cursor is opaque to clients: base64url of "createdAt|paymentId" from the last item.
 * Each item carries the reasons and riskScore of the payment's most recently recorded
 * verdict (screening_verdicts.received_at), or null for both if it has none.
 */
@Repository
public class HeldPaymentQuery {

    private static final String SELECT =
            "SELECT p.payment_id, p.amount, p.currency, p.from_account, p.to_account, p.created_at,"
                    + " p.overdue_at IS NOT NULL AS overdue, v.reasons::text AS reasons, v.risk_score"
                    + " FROM payments p"
                    + " LEFT JOIN LATERAL (SELECT sv.reasons, sv.risk_score FROM screening_verdicts sv"
                    + "   WHERE sv.payment_id = p.payment_id"
                    + "   ORDER BY sv.received_at DESC, sv.decided_at DESC, sv.event_id DESC LIMIT 1) v ON true"
                    + " WHERE p.status = 'HELD'";
    private static final TypeReference<List<String>> STRING_LIST = new TypeReference<>() { };
    private static final char SEPARATOR = '|';

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    public HeldPaymentQuery(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
    }

    /** overdue null = all; cursor null = first page. Fetches limit + 1 rows to know if there is a next page. */
    public Page find(Boolean overdue, String cursor, int limit) {
        StringBuilder sql = new StringBuilder(SELECT);
        List<Object> args = new ArrayList<>();
        if (overdue != null) {
            sql.append(overdue ? " AND p.overdue_at IS NOT NULL" : " AND p.overdue_at IS NULL");
        }
        if (cursor != null) {
            Position after = decode(cursor);
            sql.append(" AND (p.created_at, p.payment_id) > (?, ?)");
            args.add(Timestamp.from(after.heldSince()));
            args.add(after.paymentId());
        }
        sql.append(" ORDER BY p.created_at, p.payment_id LIMIT ?");
        args.add(limit + 1);

        List<HeldPayment> rows = jdbcTemplate.query(sql.toString(), (rs, rowNum) -> map(rs), args.toArray());
        if (rows.size() <= limit) {
            return new Page(rows, null);
        }
        List<HeldPayment> items = rows.subList(0, limit);
        HeldPayment last = items.get(limit - 1);
        return new Page(List.copyOf(items), encode(new Position(last.heldSince(), last.paymentId())));
    }

    private HeldPayment map(ResultSet rs) throws SQLException {
        return new HeldPayment(rs.getString("payment_id"), rs.getBigDecimal("amount"), rs.getString("currency"),
                rs.getString("from_account"), rs.getString("to_account"), rs.getTimestamp("created_at").toInstant(),
                rs.getBoolean("overdue"), reasons(rs.getString("reasons")), rs.getObject("risk_score", Double.class));
    }

    private List<String> reasons(String json) {
        if (json == null) {
            return null;
        }
        try {
            return objectMapper.readValue(json, STRING_LIST);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Unreadable reasons in screening_verdicts: " + json, e);
        }
    }

    static String encode(Position position) {
        String raw = position.heldSince().toString() + SEPARATOR + position.paymentId();
        return Base64.getUrlEncoder().withoutPadding().encodeToString(raw.getBytes(StandardCharsets.UTF_8));
    }

    static Position decode(String cursor) {
        try {
            String raw = new String(Base64.getUrlDecoder().decode(cursor), StandardCharsets.UTF_8);
            int separator = raw.indexOf(SEPARATOR);
            if (separator <= 0 || separator == raw.length() - 1) {
                throw new InvalidQueryException("cursor is not a cursor returned by this endpoint");
            }
            return new Position(Instant.parse(raw.substring(0, separator)), raw.substring(separator + 1));
        } catch (IllegalArgumentException | DateTimeParseException e) {
            throw new InvalidQueryException("cursor is not a cursor returned by this endpoint");
        }
    }

    public record HeldPayment(String paymentId, BigDecimal amount, String currency, String fromAccount,
                              String toAccount, Instant heldSince, boolean overdue, List<String> reasons,
                              Double riskScore) {
    }

    public record Page(List<HeldPayment> items, String nextCursor) {
    }

    record Position(Instant heldSince, String paymentId) {
    }
}
