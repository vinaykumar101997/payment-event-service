package com.example.paymentevent.repository;

import com.example.paymentevent.domain.OutboxEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.List;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, Long> {

    /**
     * Claims up to :limit pending rows that are due for a (re)try, oldest first. FOR UPDATE
     * SKIP LOCKED means a concurrent claim - another relay instance, or this one's next
     * tick overlapping - silently skips rows already claimed instead of blocking on them
     * or publishing them twice. The locks last until the caller's transaction ends, so this
     * must run inside one (OutboxRelay's).
     */
    @Query(value = "SELECT * FROM outbox_events "
            + "WHERE published_at IS NULL AND dead_at IS NULL AND next_attempt_at <= :now "
            + "ORDER BY id LIMIT :limit FOR UPDATE SKIP LOCKED", nativeQuery = true)
    List<OutboxEvent> claimPending(@Param("now") Instant now, @Param("limit") int limit);

    List<OutboxEvent> findByMessageKeyAndTopic(String messageKey, String topic);

    /** Rows that exhausted their publish attempts - for investigation and manual requeueing. */
    List<OutboxEvent> findByDeadAtIsNotNull();
}
