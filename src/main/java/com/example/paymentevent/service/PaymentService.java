package com.example.paymentevent.service;

import com.example.paymentevent.domain.Account;
import com.example.paymentevent.domain.LedgerEntry;
import com.example.paymentevent.domain.Payment;
import com.example.paymentevent.domain.PaymentStatus;
import com.example.paymentevent.exception.AccountNotFoundException;
import com.example.paymentevent.exception.CurrencyMismatchException;
import com.example.paymentevent.exception.DuplicatePaymentException;
import com.example.paymentevent.exception.InsufficientFundsException;
import com.example.paymentevent.exception.PaymentEventMismatchException;
import com.example.paymentevent.exception.PaymentValidationException;
import com.example.paymentevent.exception.UnknownAccountException;
import com.example.paymentevent.kafka.PaymentEvent;
import com.example.paymentevent.kafka.PaymentFailedEvent;
import com.example.paymentevent.kafka.TransactionEvent;
import com.example.paymentevent.outbox.OutboxWriter;
import com.example.paymentevent.repository.AccountRepository;
import com.example.paymentevent.repository.LedgerEntryRepository;
import com.example.paymentevent.repository.PaymentRepository;
import org.hibernate.exception.ConstraintViolationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

@Service
public class PaymentService {

    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);

    private static final String UNIQUE_VIOLATION = "23505";
    private static final String FOREIGN_KEY_VIOLATION = "23503";
    private static final int MAX_FAILURE_REASON_LENGTH = 500;

    private final PaymentRepository paymentRepository;
    private final AccountRepository accountRepository;
    private final LedgerEntryRepository ledgerEntryRepository;
    private final OutboxWriter outboxWriter;
    private final MoneyRules moneyRules;
    private final int maxProcessingAttempts;

    public PaymentService(PaymentRepository paymentRepository, AccountRepository accountRepository,
                          LedgerEntryRepository ledgerEntryRepository, OutboxWriter outboxWriter,
                          MoneyRules moneyRules,
                          @Value("${payments.processing.max-attempts:10}") int maxProcessingAttempts) {
        this.paymentRepository = paymentRepository;
        this.accountRepository = accountRepository;
        this.ledgerEntryRepository = ledgerEntryRepository;
        this.outboxWriter = outboxWriter;
        this.moneyRules = moneyRules;
        this.maxProcessingAttempts = maxProcessingAttempts;
    }

    /**
     * Called from the REST layer. Checks the money rules (MoneyRules: decimal places per
     * currency, maximum amount) first, then persists the payment in RECEIVED state and, in the same
     * transaction, writes its "payments" event to the outbox - so the row and the intent to
     * publish it commit together, and OutboxRelay does the actual Kafka publish afterwards.
     *
     * saveAndFlush, not save: a plain save() only queues the INSERT in the persistence
     * context - Hibernate wouldn't actually execute it (and surface a constraint
     * violation) until the transaction commits, which happens *after* this method
     * returns, well outside this try/catch. Flushing immediately forces the INSERT (and
     * any duplicate-key violation) to happen right here, where it can still be translated
     * into DuplicatePaymentException instead of escaping as an unhandled 500 later.
     *
     * Not every DataIntegrityViolationException is a duplicate: from_account/to_account are
     * foreign keys to accounts, so an unknown account fails the same INSERT. The SQLState
     * tells them apart (23505 unique_violation vs 23503 foreign_key_violation); anything
     * else is rethrown untouched rather than guessed at.
     */
    @Transactional
    public Payment createPayment(String paymentId, String fromAccount, String toAccount,
                                  BigDecimal amount, String currency) {
        moneyRules.validate(amount, currency);

        Payment payment;
        try {
            payment = paymentRepository.saveAndFlush(new Payment(paymentId, fromAccount, toAccount, amount, currency));
        } catch (DataIntegrityViolationException ex) {
            String sqlState = sqlStateOf(ex);
            if (UNIQUE_VIOLATION.equals(sqlState)) {
                throw new DuplicatePaymentException(paymentId);
            }
            if (FOREIGN_KEY_VIOLATION.equals(sqlState)) {
                throw unknownAccount(ex, fromAccount, toAccount);
            }
            throw ex;
        }
        outboxWriter.enqueuePaymentEvent(PaymentEvent.from(payment));
        return payment;
    }

    private static String sqlStateOf(Throwable ex) {
        for (Throwable t = ex; t != null; t = t.getCause()) {
            if (t instanceof SQLException sqlException && sqlException.getSQLState() != null) {
                return sqlException.getSQLState();
            }
        }
        return null;
    }

    /**
     * Postgres' default FK names (payments_from_account_fkey / payments_to_account_fkey, from
     * V1__init.sql) say which column failed. If the name can't be resolved, the exception
     * still reports both ids rather than blaming the wrong one.
     */
    private static UnknownAccountException unknownAccount(DataIntegrityViolationException ex,
                                                          String fromAccount, String toAccount) {
        String constraint = null;
        for (Throwable t = ex; t != null; t = t.getCause()) {
            if (t instanceof ConstraintViolationException cve && cve.getConstraintName() != null) {
                constraint = cve.getConstraintName().toLowerCase(Locale.ROOT);
                break;
            }
        }
        if (constraint != null && constraint.contains("from_account")) {
            return new UnknownAccountException(fromAccount);
        }
        if (constraint != null && constraint.contains("to_account")) {
            return new UnknownAccountException(toAccount);
        }
        return new UnknownAccountException(fromAccount, toAccount);
    }

    /**
     * Called from the Kafka consumer. This is the transactional boundary for the whole
     * claim -> lock -> validate -> debit/credit sequence:
     *   1. Atomically claim the payment (RECEIVED -> PROCESSING). If we don't win the
     *      claim, this is either a duplicate delivery of an already-terminal payment or
     *      a race with another claim attempt - either way, no debit/credit here.
     *      Once claimed, only the stored Payment row's fields are used; an event that
     *      disagrees with the row is failed and sent to the DLT without moving money - a
     *      fault (tampering, corruption, a publisher bug), not a business outcome.
     *   2. Lock both accounts, always in the same sorted order, so a concurrent transfer
     *      running in the opposite direction can never deadlock against this one.
     *   3. Validate existence/currency/balance only now, under the lock - checking before
     *      locking would leave a window for the balance to change between check and use.
     *   4. Debit/credit, write the matching debit and credit ledger entries and the
     *      "transactions" outbox event, and mark PROCESSED - or, if a business rule failed
     *      (insufficient funds, currency mismatch, unknown account), mark FAILED, write a
     *      "payment.failed" outbox event, and return normally. A business failure is an
     *      expected outcome, not a fault, so it's never retried and never reaches the DLT.
     * Every FAILED payment gets a payment.failed event, including the mismatch case in step 1,
     * which additionally throws so the suspect message is parked on the DLT (noRollbackFor
     * keeps the FAILED write and its event even though we throw).
     */
    @Transactional(noRollbackFor = PaymentValidationException.class)
    public PaymentProcessingResult process(PaymentEvent event) {
        String paymentId = event.paymentId();

        if (paymentRepository.claimForProcessing(paymentId) == 0) {
            return handleUnclaimed(paymentId);
        }

        Payment payment = paymentRepository.findById(paymentId)
                .orElseThrow(() -> new IllegalStateException(
                        "Payment " + paymentId + " vanished immediately after being claimed"));

        // From here on only the stored row drives money movement. An event that disagrees
        // with it is never "corrected" to the row's values - it means the event can't be
        // trusted, so the payment is failed for investigation and nothing moves.
        String mismatch = describeMismatch(event, payment);
        if (mismatch != null) {
            String reason = "Event does not match stored payment: " + mismatch;
            payment.markFailed(reason);
            paymentRepository.save(payment);
            outboxWriter.enqueuePaymentFailedEvent(toPaymentFailedEvent(payment));
            log.error("Payment {} rejected, no money moved: {}", paymentId, reason);
            throw new PaymentEventMismatchException(reason);
        }

        AccountPair accounts = lockAccountsInOrder(payment.getFromAccount(), payment.getToAccount());

        try {
            validate(payment, accounts.from(), accounts.to());
        } catch (PaymentValidationException ex) {
            payment.markFailed(ex.getMessage());
            paymentRepository.save(payment);
            outboxWriter.enqueuePaymentFailedEvent(toPaymentFailedEvent(payment));
            log.warn("Payment {} failed validation: {}", paymentId, ex.getMessage());
            return PaymentProcessingResult.failed();
        }

        // The ledger entries are written in this same transaction as the balance change, so
        // the two can never disagree: both commit or neither does (see ReconciliationService).
        accounts.from().debit(payment.getAmount());
        accounts.to().credit(payment.getAmount());
        ledgerEntryRepository.saveAll(List.of(LedgerEntry.debit(payment), LedgerEntry.credit(payment)));
        payment.markProcessed();
        // Same transaction again: the "transactions" event commits with the money movement,
        // so there is no window in which one is durable and the other isn't.
        outboxWriter.enqueueTransactionEvent(toTransactionEvent(payment));

        log.info("Payment {} processed: {} -> {} amount={} {}",
                paymentId, payment.getFromAccount(), payment.getToAccount(), payment.getAmount(), payment.getCurrency());

        return PaymentProcessingResult.processedNow();
    }

    /**
     * Returns a description of every field where the event disagrees with the stored row,
     * or null if they match. Amount is compared with compareTo so 100.0 and 100.00 match.
     */
    private String describeMismatch(PaymentEvent event, Payment payment) {
        StringBuilder diff = new StringBuilder();
        if (!Objects.equals(event.fromAccount(), payment.getFromAccount())) {
            appendDiff(diff, "fromAccount", event.fromAccount(), payment.getFromAccount());
        }
        if (!Objects.equals(event.toAccount(), payment.getToAccount())) {
            appendDiff(diff, "toAccount", event.toAccount(), payment.getToAccount());
        }
        if (event.amount() == null || event.amount().compareTo(payment.getAmount()) != 0) {
            appendDiff(diff, "amount", event.amount(), payment.getAmount());
        }
        if (!Objects.equals(event.currency(), payment.getCurrency())) {
            appendDiff(diff, "currency", event.currency(), payment.getCurrency());
        }
        return diff.length() == 0 ? null : diff.toString();
    }

    private static void appendDiff(StringBuilder diff, String field, Object eventValue, Object storedValue) {
        if (diff.length() > 0) {
            diff.append("; ");
        }
        diff.append(field).append(" event=").append(eventValue).append(" stored=").append(storedValue);
    }

    /**
     * Called by PaymentConsumer when process() failed with a non-business error (anything but
     * a PaymentValidationException). That attempt's transaction - claim included - has rolled
     * back, so the count is written in a transaction of its own (REQUIRES_NEW) or it would be
     * lost with it. Once the count reaches payments.processing.max-attempts the payment moves
     * to the terminal POISONED status, so neither Kafka retries nor PaymentSweeper keep
     * feeding it back in forever. Returns true if this call is the one that poisoned it.
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public boolean recordFailedAttempt(String paymentId, Throwable error) {
        if (paymentRepository.incrementProcessingAttempts(paymentId) == 0) {
            return false;
        }
        String reason = "Gave up after " + maxProcessingAttempts + " failed processing attempts; last error: "
                + rootCauseMessage(error);
        if (reason.length() > MAX_FAILURE_REASON_LENGTH) {
            reason = reason.substring(0, MAX_FAILURE_REASON_LENGTH);
        }
        boolean poisoned = paymentRepository.markPoisonedIfExhausted(
                paymentId, maxProcessingAttempts, reason, Instant.now()) == 1;
        if (poisoned) {
            log.error("Payment {} POISONED: {}", paymentId, reason);
        } else {
            log.warn("Processing attempt for payment {} failed and will be retried: {}",
                    paymentId, rootCauseMessage(error));
        }
        return poisoned;
    }

    private static String rootCauseMessage(Throwable error) {
        Throwable root = error;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        return root.getClass().getSimpleName() + ": " + root.getMessage();
    }

    private PaymentProcessingResult handleUnclaimed(String paymentId) {
        Payment existing = paymentRepository.findById(paymentId)
                .orElseThrow(() -> new IllegalStateException(
                        "Payment " + paymentId + " not found; it must be created via POST /payments "
                                + "before its event reaches this consumer"));

        if (existing.getStatus() == PaymentStatus.PROCESSED) {
            log.info("Duplicate delivery for already-processed payment {}; its transaction event is already "
                    + "in the outbox, nothing to do", paymentId);
            return PaymentProcessingResult.alreadyProcessed();
        }

        log.info("Ignoring duplicate/concurrent event for payment {} (current status={})",
                paymentId, existing.getStatus());
        return PaymentProcessingResult.skipped();
    }

    private AccountPair lockAccountsInOrder(String fromId, String toId) {
        boolean fromFirst = fromId.compareTo(toId) < 0;
        String firstId = fromFirst ? fromId : toId;
        String secondId = fromFirst ? toId : fromId;

        Account first = accountRepository.findByIdForUpdate(firstId).orElse(null);
        Account second = accountRepository.findByIdForUpdate(secondId).orElse(null);

        return fromFirst ? new AccountPair(first, second) : new AccountPair(second, first);
    }

    private void validate(Payment payment, Account from, Account to) {
        if (from == null) {
            throw new AccountNotFoundException(payment.getFromAccount());
        }
        if (to == null) {
            throw new AccountNotFoundException(payment.getToAccount());
        }
        if (!from.getCurrency().equals(payment.getCurrency()) || !to.getCurrency().equals(payment.getCurrency())) {
            throw new CurrencyMismatchException("Payment currency " + payment.getCurrency()
                    + " does not match account currency (from=" + from.getCurrency()
                    + ", to=" + to.getCurrency() + ")");
        }
        if (from.getBalance().compareTo(payment.getAmount()) < 0) {
            throw new InsufficientFundsException("Account " + from.getId()
                    + " has insufficient funds for amount " + payment.getAmount());
        }
    }

    private TransactionEvent toTransactionEvent(Payment payment) {
        return new TransactionEvent(payment.getPaymentId(), payment.getFromAccount(), payment.getToAccount(),
                payment.getAmount(), payment.getCurrency(), payment.getProcessedAt());
    }

    private PaymentFailedEvent toPaymentFailedEvent(Payment payment) {
        return new PaymentFailedEvent(payment.getPaymentId(), payment.getFromAccount(), payment.getToAccount(),
                payment.getAmount(), payment.getCurrency(), payment.getFailureReason(), payment.getProcessedAt());
    }

    private record AccountPair(Account from, Account to) {
    }
}
