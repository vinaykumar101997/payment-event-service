package com.example.paymentevent.outbox;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;

/**
 * Deletes published outbox rows once they're older than outbox.retention.days. After Kafka has
 * a row's event, the row is only an audit trail, and without this the table grows forever.
 *
 * Only published rows qualify. Dead rows (publishing gave up; published_at stays NULL) are kept
 * for investigation and manual requeueing, and pending rows are still work for OutboxRelay -
 * neither is ever deleted, however old.
 *
 * Deletes run in batches of outbox.retention.batch-size, each in its own short transaction, so
 * a large backlog never holds one long transaction or a lock on millions of rows. The batch is
 * picked in id order (the primary key index, which starts at the oldest rows) with SKIP LOCKED,
 * so it never waits on a row another transaction holds.
 */
@Component
public class OutboxRetentionJob {

    private static final Logger log = LoggerFactory.getLogger(OutboxRetentionJob.class);

    private static final String DELETE_BATCH_SQL =
            "DELETE FROM outbox_events WHERE id IN ("
                    + "  SELECT id FROM outbox_events"
                    + "  WHERE published_at IS NOT NULL AND published_at < ? AND dead_at IS NULL"
                    + "  ORDER BY id LIMIT ? FOR UPDATE SKIP LOCKED)";

    private final JdbcTemplate jdbcTemplate;
    private final TransactionTemplate transactionTemplate;
    private final Duration retention;
    private final int batchSize;

    public OutboxRetentionJob(JdbcTemplate jdbcTemplate, TransactionTemplate transactionTemplate,
                              @Value("${outbox.retention.days:30}") int retentionDays,
                              @Value("${outbox.retention.batch-size:1000}") int batchSize) {
        if (retentionDays < 1) {
            throw new IllegalArgumentException("outbox.retention.days must be at least 1, was " + retentionDays);
        }
        if (batchSize < 1) {
            throw new IllegalArgumentException("outbox.retention.batch-size must be at least 1, was " + batchSize);
        }
        this.jdbcTemplate = jdbcTemplate;
        this.transactionTemplate = transactionTemplate;
        this.retention = Duration.ofDays(retentionDays);
        this.batchSize = batchSize;
    }

    @Scheduled(fixedDelayString = "${outbox.retention.interval-ms:3600000}",
            initialDelayString = "${outbox.retention.interval-ms:3600000}")
    public void scheduledPurge() {
        purge();
    }

    /** Deletes every qualifying row, batch by batch, until a batch comes back short. */
    public PurgeResult purge() {
        Timestamp cutoff = Timestamp.from(Instant.now().minus(retention));
        int total = 0;
        int batches = 0;
        int deleted;
        do {
            Integer count = transactionTemplate.execute(status ->
                    jdbcTemplate.update(DELETE_BATCH_SQL, cutoff, batchSize));
            deleted = count == null ? 0 : count;
            total += deleted;
            if (deleted > 0) {
                batches++;
            }
        } while (deleted == batchSize);

        if (total > 0) {
            log.info("Outbox retention deleted {} published rows older than {} in {} batches",
                    total, cutoff.toInstant(), batches);
        }
        return new PurgeResult(total, batches);
    }

    public record PurgeResult(int rowsDeleted, int batches) {
    }
}
