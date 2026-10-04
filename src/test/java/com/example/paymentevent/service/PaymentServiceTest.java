package com.example.paymentevent.service;

import com.example.paymentevent.domain.Account;
import com.example.paymentevent.domain.LedgerEntry;
import com.example.paymentevent.domain.Payment;
import com.example.paymentevent.domain.PaymentStatus;
import com.example.paymentevent.exception.DuplicatePaymentException;
import com.example.paymentevent.exception.InvalidAmountException;
import com.example.paymentevent.exception.PaymentValidationException;
import com.example.paymentevent.kafka.PaymentEvent;
import com.example.paymentevent.kafka.PaymentFailedEvent;
import com.example.paymentevent.kafka.TransactionEvent;
import com.example.paymentevent.outbox.OutboxWriter;
import com.example.paymentevent.repository.AccountRepository;
import com.example.paymentevent.repository.LedgerEntryRepository;
import com.example.paymentevent.repository.PaymentRepository;
import org.hibernate.exception.ConstraintViolationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataIntegrityViolationException;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PaymentServiceTest {

    @Mock
    private PaymentRepository paymentRepository;

    @Mock
    private AccountRepository accountRepository;

    @Mock
    private LedgerEntryRepository ledgerEntryRepository;

    @Mock
    private OutboxWriter outboxWriter;

    private PaymentService paymentService;

    @BeforeEach
    void setUp() {
        paymentService = new PaymentService(paymentRepository, accountRepository, ledgerEntryRepository,
                outboxWriter, new MoneyRules(new BigDecimal("1000000.00")), 10);
    }

    @Test
    void processDebitsAndCreditsWhenClaimSucceedsAndFundsAreSufficient() {
        Payment payment = new Payment("p1", "acc-a", "acc-b", new BigDecimal("100.00"), "USD");
        Account from = new Account("acc-a", new BigDecimal("500.00"), "USD");
        Account to = new Account("acc-b", new BigDecimal("0.00"), "USD");

        when(paymentRepository.claimForProcessing("p1")).thenReturn(1);
        when(paymentRepository.findById("p1")).thenReturn(Optional.of(payment));
        when(accountRepository.findByIdForUpdate("acc-a")).thenReturn(Optional.of(from));
        when(accountRepository.findByIdForUpdate("acc-b")).thenReturn(Optional.of(to));

        PaymentProcessingResult result = paymentService.process(
                new PaymentEvent("p1", "acc-a", "acc-b", new BigDecimal("100.00"), "USD"));

        assertThat(result.outcome()).isEqualTo(PaymentProcessingResult.Outcome.PROCESSED_NOW);
        assertThat(from.getBalance()).isEqualByComparingTo("400.00");
        assertThat(to.getBalance()).isEqualByComparingTo("100.00");
        assertThat(payment.getStatus()).isEqualTo(PaymentStatus.PROCESSED);
    }

    /**
     * Double-entry: a processed payment posts exactly one debit (from account, negative) and
     * one credit (to account, positive) for the stored amount, summing to zero.
     */
    @Test
    @SuppressWarnings("unchecked")
    void processWritesOneDebitAndOneCreditLedgerEntrySummingToZero() {
        Payment payment = new Payment("p1", "acc-a", "acc-b", new BigDecimal("100.00"), "USD");
        Account from = new Account("acc-a", new BigDecimal("500.00"), "USD");
        Account to = new Account("acc-b", new BigDecimal("0.00"), "USD");

        when(paymentRepository.claimForProcessing("p1")).thenReturn(1);
        when(paymentRepository.findById("p1")).thenReturn(Optional.of(payment));
        when(accountRepository.findByIdForUpdate("acc-a")).thenReturn(Optional.of(from));
        when(accountRepository.findByIdForUpdate("acc-b")).thenReturn(Optional.of(to));

        paymentService.process(new PaymentEvent("p1", "acc-a", "acc-b", new BigDecimal("100.00"), "USD"));

        ArgumentCaptor<List<LedgerEntry>> captor = ArgumentCaptor.forClass(List.class);
        verify(ledgerEntryRepository).saveAll(captor.capture());
        List<LedgerEntry> entries = captor.getValue();

        assertThat(entries).hasSize(2);
        assertThat(entries).allSatisfy(e -> {
            assertThat(e.getPaymentId()).isEqualTo("p1");
            assertThat(e.getCurrency()).isEqualTo("USD");
        });
        assertThat(entries).anySatisfy(e -> {
            assertThat(e.getAccountId()).isEqualTo("acc-a");
            assertThat(e.getAmount()).isEqualByComparingTo("-100.00");
        });
        assertThat(entries).anySatisfy(e -> {
            assertThat(e.getAccountId()).isEqualTo("acc-b");
            assertThat(e.getAmount()).isEqualByComparingTo("100.00");
        });
        assertThat(entries.stream().map(LedgerEntry::getAmount).reduce(BigDecimal.ZERO, BigDecimal::add))
                .isEqualByComparingTo("0");
    }

    @Test
    void processWritesNoLedgerEntriesWhenValidationFails() {
        Payment payment = new Payment("p1", "acc-a", "acc-b", new BigDecimal("100.00"), "USD");
        Account from = new Account("acc-a", new BigDecimal("10.00"), "USD");
        Account to = new Account("acc-b", new BigDecimal("0.00"), "USD");

        when(paymentRepository.claimForProcessing("p1")).thenReturn(1);
        when(paymentRepository.findById("p1")).thenReturn(Optional.of(payment));
        when(accountRepository.findByIdForUpdate("acc-a")).thenReturn(Optional.of(from));
        when(accountRepository.findByIdForUpdate("acc-b")).thenReturn(Optional.of(to));

        paymentService.process(new PaymentEvent("p1", "acc-a", "acc-b", new BigDecimal("100.00"), "USD"));

        verify(ledgerEntryRepository, never()).saveAll(anyList());
        verify(outboxWriter, never()).enqueueTransactionEvent(any());
    }

    /**
     * The "transactions" event is written to the outbox by process() itself, in the same
     * transaction as the debit/credit, rather than published by the listener afterwards -
     * so it commits atomically with the money movement and can't be lost between the two.
     */
    @Test
    void processEnqueuesTransactionEventToOutbox() {
        Payment payment = new Payment("p1", "acc-a", "acc-b", new BigDecimal("100.00"), "USD");
        Account from = new Account("acc-a", new BigDecimal("500.00"), "USD");
        Account to = new Account("acc-b", new BigDecimal("0.00"), "USD");

        when(paymentRepository.claimForProcessing("p1")).thenReturn(1);
        when(paymentRepository.findById("p1")).thenReturn(Optional.of(payment));
        when(accountRepository.findByIdForUpdate("acc-a")).thenReturn(Optional.of(from));
        when(accountRepository.findByIdForUpdate("acc-b")).thenReturn(Optional.of(to));

        paymentService.process(new PaymentEvent("p1", "acc-a", "acc-b", new BigDecimal("100.00"), "USD"));

        ArgumentCaptor<TransactionEvent> captor = ArgumentCaptor.forClass(TransactionEvent.class);
        verify(outboxWriter).enqueueTransactionEvent(captor.capture());
        TransactionEvent event = captor.getValue();
        assertThat(event.paymentId()).isEqualTo("p1");
        assertThat(event.fromAccount()).isEqualTo("acc-a");
        assertThat(event.toAccount()).isEqualTo("acc-b");
        assertThat(event.amount()).isEqualByComparingTo("100.00");
        assertThat(event.currency()).isEqualTo("USD");
        assertThat(event.processedAt()).isEqualTo(payment.getProcessedAt()).isNotNull();
    }

    /**
     * A business-rule failure is a final outcome, not a fault: the payment is marked FAILED,
     * a payment.failed event is enqueued in the same transaction, and process() returns
     * normally - nothing is thrown, so the message is not retried and never reaches the DLT.
     */
    @Test
    void processMarksFailedAndEnqueuesPaymentFailedOnInsufficientFundsWithoutThrowing() {
        Payment payment = new Payment("p1", "acc-a", "acc-b", new BigDecimal("100.00"), "USD");
        Account from = new Account("acc-a", new BigDecimal("10.00"), "USD");
        Account to = new Account("acc-b", new BigDecimal("0.00"), "USD");

        when(paymentRepository.claimForProcessing("p1")).thenReturn(1);
        when(paymentRepository.findById("p1")).thenReturn(Optional.of(payment));
        when(accountRepository.findByIdForUpdate("acc-a")).thenReturn(Optional.of(from));
        when(accountRepository.findByIdForUpdate("acc-b")).thenReturn(Optional.of(to));

        PaymentProcessingResult result = paymentService.process(
                new PaymentEvent("p1", "acc-a", "acc-b", new BigDecimal("100.00"), "USD"));

        assertThat(result.outcome()).isEqualTo(PaymentProcessingResult.Outcome.FAILED);
        assertThat(payment.getStatus()).isEqualTo(PaymentStatus.FAILED);
        assertThat(from.getBalance()).isEqualByComparingTo("10.00");
        verify(paymentRepository).save(payment);
        PaymentFailedEvent event = capturePaymentFailedEvent();
        assertThat(event.paymentId()).isEqualTo("p1");
        assertThat(event.amount()).isEqualByComparingTo("100.00");
        assertThat(event.reason()).containsIgnoringCase("insufficient funds");
        assertThat(event.failedAt()).isEqualTo(payment.getProcessedAt()).isNotNull();
    }

    @Test
    void processMarksFailedAndEnqueuesPaymentFailedOnUnknownAccountWithoutThrowing() {
        Payment payment = new Payment("p1", "acc-a", "acc-missing", new BigDecimal("100.00"), "USD");
        Account from = new Account("acc-a", new BigDecimal("500.00"), "USD");

        when(paymentRepository.claimForProcessing("p1")).thenReturn(1);
        when(paymentRepository.findById("p1")).thenReturn(Optional.of(payment));
        when(accountRepository.findByIdForUpdate("acc-a")).thenReturn(Optional.of(from));
        when(accountRepository.findByIdForUpdate("acc-missing")).thenReturn(Optional.empty());

        PaymentProcessingResult result = paymentService.process(
                new PaymentEvent("p1", "acc-a", "acc-missing", new BigDecimal("100.00"), "USD"));

        assertThat(result.outcome()).isEqualTo(PaymentProcessingResult.Outcome.FAILED);
        assertThat(payment.getStatus()).isEqualTo(PaymentStatus.FAILED);
        assertThat(capturePaymentFailedEvent().reason()).contains("acc-missing");
    }

    @Test
    void processMarksFailedAndEnqueuesPaymentFailedOnCurrencyMismatchWithoutThrowing() {
        Payment payment = new Payment("p1", "acc-a", "acc-b", new BigDecimal("100.00"), "EUR");
        Account from = new Account("acc-a", new BigDecimal("500.00"), "USD");
        Account to = new Account("acc-b", new BigDecimal("0.00"), "USD");

        when(paymentRepository.claimForProcessing("p1")).thenReturn(1);
        when(paymentRepository.findById("p1")).thenReturn(Optional.of(payment));
        when(accountRepository.findByIdForUpdate("acc-a")).thenReturn(Optional.of(from));
        when(accountRepository.findByIdForUpdate("acc-b")).thenReturn(Optional.of(to));

        PaymentProcessingResult result = paymentService.process(
                new PaymentEvent("p1", "acc-a", "acc-b", new BigDecimal("100.00"), "EUR"));

        assertThat(result.outcome()).isEqualTo(PaymentProcessingResult.Outcome.FAILED);
        assertThat(payment.getStatus()).isEqualTo(PaymentStatus.FAILED);
        assertThat(capturePaymentFailedEvent().reason()).contains("does not match account currency");
    }

    private PaymentFailedEvent capturePaymentFailedEvent() {
        ArgumentCaptor<PaymentFailedEvent> captor = ArgumentCaptor.forClass(PaymentFailedEvent.class);
        verify(outboxWriter).enqueuePaymentFailedEvent(captor.capture());
        return captor.getValue();
    }

    @Test
    void processSkipsWithoutTouchingAccountsWhenClaimIsLostAndPaymentIsAlreadyFailed() {
        Payment payment = new Payment("p1", "acc-a", "acc-b", new BigDecimal("100.00"), "USD");
        payment.markFailed("insufficient funds");

        when(paymentRepository.claimForProcessing("p1")).thenReturn(0);
        when(paymentRepository.findById("p1")).thenReturn(Optional.of(payment));

        PaymentProcessingResult result = paymentService.process(
                new PaymentEvent("p1", "acc-a", "acc-b", new BigDecimal("100.00"), "USD"));

        assertThat(result.outcome()).isEqualTo(PaymentProcessingResult.Outcome.SKIPPED);
        verifyNoInteractions(accountRepository);
    }

    /**
     * A redelivery of an already-processed payment repeats neither the debit/credit nor the
     * "transactions" event: the event was enqueued in the outbox by the original processing
     * transaction, so enqueuing it again would just publish a duplicate.
     */
    @Test
    void processDoesNotRepeatDebitCreditOrReenqueueWhenAlreadyProcessed() {
        Payment payment = new Payment("p1", "acc-a", "acc-b", new BigDecimal("100.00"), "USD");
        payment.markProcessed();

        when(paymentRepository.claimForProcessing("p1")).thenReturn(0);
        when(paymentRepository.findById("p1")).thenReturn(Optional.of(payment));

        PaymentProcessingResult result = paymentService.process(
                new PaymentEvent("p1", "acc-a", "acc-b", new BigDecimal("100.00"), "USD"));

        assertThat(result.outcome()).isEqualTo(PaymentProcessingResult.Outcome.ALREADY_PROCESSED);
        verifyNoInteractions(accountRepository, outboxWriter);
    }

    @Test
    void createPaymentPersistsInReceivedState() {
        when(paymentRepository.saveAndFlush(any(Payment.class))).thenAnswer(invocation -> invocation.getArgument(0));

        Payment payment = paymentService.createPayment("p1", "acc-a", "acc-b", new BigDecimal("50.00"), "USD");

        assertThat(payment.getStatus()).isEqualTo(PaymentStatus.RECEIVED);
        assertThat(payment.getPaymentId()).isEqualTo("p1");
    }

    /**
     * The "payments" event is written to the outbox inside createPayment()'s transaction,
     * so the Payment row and the intent to publish it commit (or roll back) together.
     */
    @Test
    void createPaymentEnqueuesPaymentEventToOutbox() {
        when(paymentRepository.saveAndFlush(any(Payment.class))).thenAnswer(invocation -> invocation.getArgument(0));

        paymentService.createPayment("p1", "acc-a", "acc-b", new BigDecimal("50.00"), "USD");

        verify(outboxWriter).enqueuePaymentEvent(
                new PaymentEvent("p1", "acc-a", "acc-b", new BigDecimal("50.00"), "USD"));
    }

    /** Money rules run before anything is written: a rejected amount never reaches the database. */
    @Test
    void createPaymentRejectsAmountThatBreaksMoneyRulesBeforePersisting() {
        assertThatThrownBy(() -> paymentService.createPayment("p1", "acc-a", "acc-b", new BigDecimal("100.5"), "JPY"))
                .isInstanceOf(InvalidAmountException.class);
        assertThatThrownBy(() -> paymentService.createPayment("p1", "acc-a", "acc-b", new BigDecimal("1000000.01"), "USD"))
                .isInstanceOf(InvalidAmountException.class);

        verifyNoInteractions(paymentRepository, outboxWriter);
    }

    @Test
    void createPaymentDoesNotEnqueueWhenTheInsertFails() {
        when(paymentRepository.saveAndFlush(any(Payment.class))).thenThrow(
                constraintViolation("23505", "payments_pkey"));

        assertThatThrownBy(() -> paymentService.createPayment("p1", "acc-a", "acc-b", new BigDecimal("50.00"), "USD"))
                .isInstanceOf(DuplicatePaymentException.class);

        verifyNoInteractions(outboxWriter);
    }

    @Test
    void createPaymentTranslatesConstraintViolationToDuplicateException() {
        when(paymentRepository.saveAndFlush(any(Payment.class))).thenThrow(
                constraintViolation("23505", "payments_pkey"));

        assertThatThrownBy(() -> paymentService.createPayment("p1", "acc-a", "acc-b", new BigDecimal("50.00"), "USD"))
                .isInstanceOf(DuplicatePaymentException.class);
    }

    @Test
    void createPaymentTranslatesForeignKeyViolationToUnknownAccountRatherThanDuplicate() {
        when(paymentRepository.saveAndFlush(any(Payment.class))).thenThrow(
                constraintViolation("23503", "payments_to_account_fkey"));

        assertThatThrownBy(() -> paymentService.createPayment("p1", "acc-a", "acc-missing", new BigDecimal("50.00"), "USD"))
                .isNotInstanceOf(DuplicatePaymentException.class)
                .hasMessageContaining("Unknown account")
                .hasMessageContaining("acc-missing");
    }

    /**
     * An event whose fields disagree with the stored Payment row (tampered, corrupted, or
     * produced by a buggy publisher) must not move money at all - not even the row's amount,
     * since the row and the event can't both be trusted. The accounts are stubbed leniently
     * so that code which (wrongly) trusts the event has real accounts to debit/credit.
     */
    @Test
    void processRejectsEventThatDoesNotMatchStoredPaymentWithoutMovingMoney() {
        Payment payment = new Payment("p1", "acc-a", "acc-b", new BigDecimal("100.00"), "USD");
        Account from = new Account("acc-a", new BigDecimal("1000.00"), "USD");
        Account to = new Account("acc-b", new BigDecimal("0.00"), "USD");

        when(paymentRepository.claimForProcessing("p1")).thenReturn(1);
        when(paymentRepository.findById("p1")).thenReturn(Optional.of(payment));
        lenient().when(accountRepository.findByIdForUpdate("acc-a")).thenReturn(Optional.of(from));
        lenient().when(accountRepository.findByIdForUpdate("acc-b")).thenReturn(Optional.of(to));

        assertThatThrownBy(() -> paymentService.process(
                new PaymentEvent("p1", "acc-a", "acc-b", new BigDecimal("500.00"), "USD")))
                .isInstanceOf(PaymentValidationException.class)
                .hasMessageContaining("does not match");

        assertThat(from.getBalance()).isEqualByComparingTo("1000.00");
        assertThat(to.getBalance()).isEqualByComparingTo("0.00");
        assertThat(payment.getStatus()).isEqualTo(PaymentStatus.FAILED);
        verify(paymentRepository).save(payment);
        // Still thrown (it's a fault, so the message goes to the DLT), but the payment's
        // FAILED outcome is announced like any other.
        assertThat(capturePaymentFailedEvent().reason()).contains("does not match");
    }

    private static DataIntegrityViolationException constraintViolation(String sqlState, String constraintName) {
        SQLException sqlException = new SQLException("constraint " + constraintName + " violated", sqlState);
        return new DataIntegrityViolationException("could not execute statement",
                new ConstraintViolationException("could not execute statement", sqlException, constraintName));
    }
}
