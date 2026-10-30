package com.example.paymentevent.metrics;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Backlog gauges, sampled on a fixed delay (payments.metrics.refresh-interval-ms) rather than
 * per scrape, so a scrape never touches the database:
 *
 *   payments.outbox.unpublished                    outbox rows waiting to be published (dead rows excluded)
 *   payments.outbox.oldest.unpublished.age{s}      age of the oldest of them, 0 when there are none
 *   payments.screening.held                        payments currently HELD for screening
 *
 * Each query is index-only work: the pending outbox rows are covered by the partial index
 * idx_outbox_events_pending, and HELD payments by idx_payments_status. Dead outbox rows are
 * left out because they are never retried; they would keep the age growing for good.
 */
@Component
public class BacklogMetrics {

    private static final String OUTBOX_SQL =
            "SELECT count(*) AS pending, min(created_at) AS oldest FROM outbox_events "
                    + "WHERE published_at IS NULL AND dead_at IS NULL";
    private static final String HELD_SQL = "SELECT count(*) FROM payments WHERE status = 'HELD'";

    private final JdbcTemplate jdbcTemplate;
    private final AtomicLong outboxUnpublished = new AtomicLong();
    private final AtomicReference<Instant> oldestUnpublished = new AtomicReference<>();
    private final AtomicLong held = new AtomicLong();

    public BacklogMetrics(JdbcTemplate jdbcTemplate, MeterRegistry registry) {
        this.jdbcTemplate = jdbcTemplate;
        Gauge.builder("payments.outbox.unpublished", outboxUnpublished, AtomicLong::get)
                .description("Outbox rows not yet published to Kafka, dead rows excluded")
                .register(registry);
        Gauge.builder("payments.outbox.oldest.unpublished.age", this, BacklogMetrics::oldestUnpublishedAgeSeconds)
                .description("Age of the oldest unpublished outbox row, 0 when there is none")
                .baseUnit("seconds")
                .register(registry);
        Gauge.builder("payments.screening.held", held, AtomicLong::get)
                .description("Payments currently HELD for screening")
                .register(registry);
    }

    @Scheduled(fixedDelayString = "${payments.metrics.refresh-interval-ms:15000}")
    public void refresh() {
        jdbcTemplate.query(OUTBOX_SQL, rs -> {
            outboxUnpublished.set(rs.getLong("pending"));
            Timestamp oldest = rs.getTimestamp("oldest");
            oldestUnpublished.set(oldest == null ? null : oldest.toInstant());
        });
        Long heldNow = jdbcTemplate.queryForObject(HELD_SQL, Long.class);
        held.set(heldNow == null ? 0 : heldNow);
    }

    /** Measured from the last sample's oldest row to now, so it keeps rising between samples. */
    private double oldestUnpublishedAgeSeconds() {
        Instant oldest = oldestUnpublished.get();
        return oldest == null ? 0 : Math.max(0, Duration.between(oldest, Instant.now()).toMillis() / 1000.0);
    }
}
