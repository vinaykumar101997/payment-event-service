package com.example.paymentevent.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Duration;
import java.time.Instant;

/**
 * A Kafka message waiting to be (or already) published by OutboxRelay. payload is the
 * event serialized as JSON; the relay deserializes it back to its event type by topic.
 */
@Entity
@Table(name = "outbox_events")
public class OutboxEvent {

    private static final int MAX_ERROR_LENGTH = 1000;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Long id;

    @Column(name = "topic", nullable = false)
    private String topic;

    @Column(name = "message_key", nullable = false)
    private String messageKey;

    @Column(name = "payload", nullable = false, columnDefinition = "text")
    private String payload;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "published_at")
    private Instant publishedAt;

    @Column(name = "attempts", nullable = false)
    private int attempts;

    @Column(name = "next_attempt_at", nullable = false)
    private Instant nextAttemptAt;

    @Column(name = "last_error")
    private String lastError;

    @Column(name = "dead_at")
    private Instant deadAt;

    protected OutboxEvent() {
        // required by JPA
    }

    public OutboxEvent(String topic, String messageKey, String payload) {
        this.topic = topic;
        this.messageKey = messageKey;
        this.payload = payload;
        this.createdAt = Instant.now();
        this.nextAttemptAt = this.createdAt;
    }

    public Long getId() {
        return id;
    }

    public String getTopic() {
        return topic;
    }

    public String getMessageKey() {
        return messageKey;
    }

    public String getPayload() {
        return payload;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getPublishedAt() {
        return publishedAt;
    }

    public int getAttempts() {
        return attempts;
    }

    public Instant getNextAttemptAt() {
        return nextAttemptAt;
    }

    public String getLastError() {
        return lastError;
    }

    /** Set once publishing has failed the maximum number of times; the row is never retried again. */
    public Instant getDeadAt() {
        return deadAt;
    }

    public void markPublished() {
        this.attempts++;
        this.publishedAt = Instant.now();
    }

    /**
     * Records a failed publish. Once attempts reaches maxAttempts the row is marked dead and
     * never claimed again; otherwise it's retried after the backoff. Returns true if dead.
     */
    public boolean recordFailure(String error, Duration backoff, int maxAttempts) {
        this.attempts++;
        this.lastError = error != null && error.length() > MAX_ERROR_LENGTH
                ? error.substring(0, MAX_ERROR_LENGTH) : error;
        if (attempts >= maxAttempts) {
            this.deadAt = Instant.now();
            return true;
        }
        this.nextAttemptAt = Instant.now().plus(backoff);
        return false;
    }
}
