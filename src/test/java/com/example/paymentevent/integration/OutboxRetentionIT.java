package com.example.paymentevent.integration;

import com.example.paymentevent.outbox.OutboxRetentionJob;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Published outbox rows are only an audit trail once Kafka has them; the retention job
 * deletes those older than outbox.retention.days (30 by default), in batches
 * (outbox.retention.batch-size, 50 in tests). Rows that still matter are never touched:
 * dead rows (publishing gave up, kept for investigation) and pending rows (not yet published),
 * however old.
 */
class OutboxRetentionIT extends AbstractIntegrationTest {

    @Autowired
    private OutboxRetentionJob outboxRetentionJob;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private final List<Long> inserted = new ArrayList<>();

    @AfterEach
    void cleanUp() {
        for (Long id : inserted) {
            jdbcTemplate.update("DELETE FROM outbox_events WHERE id = ?", id);
        }
    }

    @Test
    void deletesOldPublishedRowsInBatchesAndKeepsNewPublishedDeadAndPendingRows() {
        List<Long> oldPublished = new ArrayList<>();
        for (int i = 0; i < 120; i++) {
            oldPublished.add(insert("now() - interval '45 days'", "now() - interval '40 days'", "NULL"));
        }
        long newPublished = insert("now() - interval '2 days'", "now() - interval '2 days'", "NULL");
        long justInsideRetention = insert("now() - interval '29 days'", "now() - interval '29 days'", "NULL");
        long oldDead = insert("now() - interval '90 days'", "NULL", "now() - interval '89 days'");
        long oldPending = insert("now() - interval '90 days'", "NULL", "NULL");

        OutboxRetentionJob.PurgeResult result = outboxRetentionJob.purge();

        assertThat(result.rowsDeleted()).isEqualTo(120);
        assertThat(result.batches()).isEqualTo(3);
        assertThat(existing(oldPublished)).isEmpty();
        assertThat(existing(List.of(newPublished, justInsideRetention, oldDead, oldPending)))
                .containsExactlyInAnyOrder(newPublished, justInsideRetention, oldDead, oldPending);

        // Nothing left to do: a second run deletes nothing.
        assertThat(outboxRetentionJob.purge().rowsDeleted()).isZero();
    }

    /**
     * next_attempt_at is a day ahead so the live OutboxRelay never claims the pending row;
     * it's only here to prove the job leaves unpublished rows alone.
     */
    private long insert(String createdAt, String publishedAt, String deadAt) {
        Long id = jdbcTemplate.queryForObject(
                "INSERT INTO outbox_events (topic, message_key, payload, created_at, published_at, dead_at, next_attempt_at) "
                        + "VALUES ('payments', ?, '{}', " + createdAt + ", " + publishedAt + ", " + deadAt
                        + ", now() + interval '1 day') RETURNING id",
                Long.class, "RETENTION-" + UUID.randomUUID());
        inserted.add(id);
        return id;
    }

    private List<Long> existing(List<Long> ids) {
        return jdbcTemplate.queryForList("SELECT id FROM outbox_events WHERE id = ANY (?)", Long.class,
                (Object) ids.toArray(Long[]::new));
    }
}
