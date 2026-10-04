package com.example.paymentevent.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.PostLoad;
import jakarta.persistence.PostPersist;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;
import org.springframework.data.domain.Persistable;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HexFormat;

/**
 * Implements Persistable because paymentId is a manually-assigned identifier, not
 * @GeneratedValue. Spring Data's default isNew() check (getId() == null) would see a
 * non-null id on every freshly-constructed Payment and route save() through merge()
 * instead of persist() - and merge() on an assigned id it can't find just inserts it
 * silently, meaning a genuine duplicate paymentId would UPDATE the existing row instead
 * of throwing the constraint violation createPayment() relies on to detect it. The
 * isNew field defaults to true for anything built via the constructor and flips to false
 * once Hibernate has actually persisted or loaded the row, so save() only ever inserts a
 * truly new payment and merges an existing one.
 */
@Entity
@Table(name = "payments")
public class Payment implements Persistable<String> {

    @Id
    @Column(name = "payment_id")
    private String paymentId;

    @Column(name = "from_account", nullable = false)
    private String fromAccount;

    @Column(name = "to_account", nullable = false)
    private String toAccount;

    @Column(name = "amount", nullable = false)
    private BigDecimal amount;

    @Column(name = "currency", nullable = false)
    private String currency;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false)
    private PaymentStatus status;

    @Column(name = "failure_reason")
    private String failureReason;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "processed_at")
    private Instant processedAt;

    @Column(name = "last_swept_at")
    private Instant lastSweptAt;

    @Column(name = "processing_attempts", nullable = false)
    private int processingAttempts;

    @Column(name = "request_hash", updatable = false)
    private String requestHash;

    @Transient
    private boolean isNew = true;

    protected Payment() {
        // required by JPA
    }

    public Payment(String paymentId, String fromAccount, String toAccount, BigDecimal amount, String currency) {
        this.paymentId = paymentId;
        this.fromAccount = fromAccount;
        this.toAccount = toAccount;
        this.amount = amount;
        this.currency = currency;
        this.status = PaymentStatus.RECEIVED;
        this.createdAt = Instant.now();
        this.requestHash = requestHashOf(paymentId, fromAccount, toAccount, amount, currency);
    }

    /**
     * Fingerprint of a create-payment request, used to tell an idempotent replay (same hash)
     * from a different request reusing the same paymentId. The amount is normalized with
     * stripTrailingZeros so 100, 100.0 and 100.00 - the same money - hash the same. Fields
     * are joined with a control character that can't appear in a validated request, so no
     * two different field combinations can produce the same input string.
     */
    public static String requestHashOf(String paymentId, String fromAccount, String toAccount,
                                       BigDecimal amount, String currency) {
        String canonical = String.join("\u001F", paymentId, fromAccount, toAccount,
                amount.stripTrailingZeros().toPlainString(), currency);
        try {
            MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(sha256.digest(canonical.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required by every Java platform", e);
        }
    }

    /**
     * Rows created before request_hash existed have none stored; their hash is recomputed
     * from the stored columns, which hold every field the hash covers.
     */
    public boolean matchesRequestHash(String candidateHash) {
        String stored = requestHash != null
                ? requestHash
                : requestHashOf(paymentId, fromAccount, toAccount, amount, currency);
        return stored.equals(candidateHash);
    }

    public String getPaymentId() {
        return paymentId;
    }

    @Override
    public String getId() {
        return paymentId;
    }

    @Override
    public boolean isNew() {
        return isNew;
    }

    @PostLoad
    @PostPersist
    void markNotNew() {
        this.isNew = false;
    }

    public String getFromAccount() {
        return fromAccount;
    }

    public String getToAccount() {
        return toAccount;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public String getCurrency() {
        return currency;
    }

    public PaymentStatus getStatus() {
        return status;
    }

    public String getFailureReason() {
        return failureReason;
    }

    public void markProcessed() {
        this.status = PaymentStatus.PROCESSED;
        this.processedAt = Instant.now();
    }

    public void markFailed(String reason) {
        this.status = PaymentStatus.FAILED;
        this.failureReason = reason;
        this.processedAt = Instant.now();
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getProcessedAt() {
        return processedAt;
    }

    public Instant getLastSweptAt() {
        return lastSweptAt;
    }

    public void markSwept() {
        this.lastSweptAt = Instant.now();
    }

    public String getRequestHash() {
        return requestHash;
    }

    /** Failed non-business processing attempts so far (see PaymentService.recordFailedAttempt). */
    public int getProcessingAttempts() {
        return processingAttempts;
    }
}
