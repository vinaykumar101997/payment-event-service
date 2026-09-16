package com.example.paymentevent.service;

import com.example.paymentevent.domain.Account;
import com.example.paymentevent.domain.Payment;
import com.example.paymentevent.domain.PaymentStatus;
import com.example.paymentevent.exception.AccountNotFoundException;
import com.example.paymentevent.exception.CurrencyMismatchException;
import com.example.paymentevent.exception.DuplicatePaymentException;
import com.example.paymentevent.exception.InsufficientFundsException;
import com.example.paymentevent.exception.PaymentValidationException;
import com.example.paymentevent.kafka.PaymentEvent;
import com.example.paymentevent.kafka.TransactionEvent;
import com.example.paymentevent.repository.AccountRepository;
import com.example.paymentevent.repository.PaymentRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;

@Service
public class PaymentService {

    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);

    private final PaymentRepository paymentRepository;
    private final AccountRepository accountRepository;

    public PaymentService(PaymentRepository paymentRepository, AccountRepository accountRepository) {
        this.paymentRepository = paymentRepository;
        this.accountRepository = accountRepository;
    }

    /**
     * Called from the REST layer. Persists the payment in RECEIVED state; the caller is
     * responsible for publishing it to the "payments" topic afterward.
     */
    @Transactional
    public Payment createPayment(String paymentId, String fromAccount, String toAccount,
                                  BigDecimal amount, String currency) {
        try {
            return paymentRepository.save(new Payment(paymentId, fromAccount, toAccount, amount, currency));
        } catch (DataIntegrityViolationException ex) {
            throw new DuplicatePaymentException(paymentId);
        }
    }

    /**
     * Called from the Kafka consumer. This is the transactional boundary for the whole
     * claim -> lock -> validate -> debit/credit sequence:
     *   1. Atomically claim the payment (RECEIVED -> PROCESSING). If we don't win the
     *      claim, this is either a duplicate delivery of an already-terminal payment or
     *      a race with another claim attempt - either way, no debit/credit here.
     *   2. Lock both accounts, always in the same sorted order, so a concurrent transfer
     *      running in the opposite direction can never deadlock against this one.
     *   3. Validate existence/currency/balance only now, under the lock - checking before
     *      locking would leave a window for the balance to change between check and use.
     *   4. Debit/credit and mark PROCESSED, or mark FAILED and rethrow so Kafka's error
     *      handler routes the original message to the DLT (noRollbackFor keeps the FAILED
     *      write even though we throw).
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

        AccountPair accounts = lockAccountsInOrder(event.fromAccount(), event.toAccount());

        try {
            validate(event, accounts.from(), accounts.to());
        } catch (PaymentValidationException ex) {
            payment.markFailed(ex.getMessage());
            paymentRepository.save(payment);
            log.warn("Payment {} failed validation: {}", paymentId, ex.getMessage());
            throw ex;
        }

        accounts.from().debit(event.amount());
        accounts.to().credit(event.amount());
        payment.markProcessed();

        log.info("Payment {} processed: {} -> {} amount={} {}",
                paymentId, event.fromAccount(), event.toAccount(), event.amount(), event.currency());

        return PaymentProcessingResult.processedNow(toTransactionEvent(payment));
    }

    private PaymentProcessingResult handleUnclaimed(String paymentId) {
        Payment existing = paymentRepository.findById(paymentId)
                .orElseThrow(() -> new IllegalStateException(
                        "Payment " + paymentId + " not found; it must be created via POST /payments "
                                + "before its event reaches this consumer"));

        if (existing.getStatus() == PaymentStatus.PROCESSED) {
            log.info("Duplicate delivery for already-processed payment {}; re-publishing to transactions "
                    + "without repeating the debit/credit", paymentId);
            return PaymentProcessingResult.alreadyProcessed(toTransactionEvent(existing));
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

    private void validate(PaymentEvent event, Account from, Account to) {
        if (from == null) {
            throw new AccountNotFoundException(event.fromAccount());
        }
        if (to == null) {
            throw new AccountNotFoundException(event.toAccount());
        }
        if (!from.getCurrency().equals(event.currency()) || !to.getCurrency().equals(event.currency())) {
            throw new CurrencyMismatchException("Payment currency " + event.currency()
                    + " does not match account currency (from=" + from.getCurrency()
                    + ", to=" + to.getCurrency() + ")");
        }
        if (from.getBalance().compareTo(event.amount()) < 0) {
            throw new InsufficientFundsException("Account " + from.getId()
                    + " has insufficient funds for amount " + event.amount());
        }
    }

    private TransactionEvent toTransactionEvent(Payment payment) {
        return new TransactionEvent(payment.getPaymentId(), payment.getFromAccount(), payment.getToAccount(),
                payment.getAmount(), payment.getCurrency(), payment.getProcessedAt());
    }

    private record AccountPair(Account from, Account to) {
    }
}
