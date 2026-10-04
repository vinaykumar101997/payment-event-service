package com.example.paymentevent.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * One side of a double-entry posting. amount is signed: negative debits the account,
 * positive credits it. A processed payment always has exactly one of each, summing to zero.
 */
@Entity
@Table(name = "ledger_entries")
public class LedgerEntry {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Long id;

    @Column(name = "payment_id", nullable = false)
    private String paymentId;

    @Column(name = "account_id", nullable = false)
    private String accountId;

    @Column(name = "amount", nullable = false)
    private BigDecimal amount;

    @Column(name = "currency", nullable = false)
    private String currency;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    protected LedgerEntry() {
        // required by JPA
    }

    private LedgerEntry(String paymentId, String accountId, BigDecimal amount, String currency) {
        this.paymentId = paymentId;
        this.accountId = accountId;
        this.amount = amount;
        this.currency = currency;
        this.createdAt = Instant.now();
    }

    public static LedgerEntry debit(Payment payment) {
        return new LedgerEntry(payment.getPaymentId(), payment.getFromAccount(),
                payment.getAmount().negate(), payment.getCurrency());
    }

    public static LedgerEntry credit(Payment payment) {
        return new LedgerEntry(payment.getPaymentId(), payment.getToAccount(),
                payment.getAmount(), payment.getCurrency());
    }

    public Long getId() {
        return id;
    }

    public String getPaymentId() {
        return paymentId;
    }

    public String getAccountId() {
        return accountId;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public String getCurrency() {
        return currency;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }
}
