package com.example.paymentevent.service;

import com.example.paymentevent.domain.Account;
import com.example.paymentevent.domain.Payment;
import com.example.paymentevent.domain.PaymentStatus;
import com.example.paymentevent.exception.AccountNotFoundException;
import com.example.paymentevent.exception.CurrencyMismatchException;
import com.example.paymentevent.exception.DuplicatePaymentException;
import com.example.paymentevent.exception.InsufficientFundsException;
import com.example.paymentevent.kafka.PaymentEvent;
import com.example.paymentevent.repository.AccountRepository;
import com.example.paymentevent.repository.PaymentRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataIntegrityViolationException;

import java.math.BigDecimal;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PaymentServiceTest {

    @Mock
    private PaymentRepository paymentRepository;

    @Mock
    private AccountRepository accountRepository;

    private PaymentService paymentService;

    @BeforeEach
    void setUp() {
        paymentService = new PaymentService(paymentRepository, accountRepository);
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

    @Test
    void processMarksFailedAndRethrowsOnInsufficientFunds() {
        Payment payment = new Payment("p1", "acc-a", "acc-b", new BigDecimal("100.00"), "USD");
        Account from = new Account("acc-a", new BigDecimal("10.00"), "USD");
        Account to = new Account("acc-b", new BigDecimal("0.00"), "USD");

        when(paymentRepository.claimForProcessing("p1")).thenReturn(1);
        when(paymentRepository.findById("p1")).thenReturn(Optional.of(payment));
        when(accountRepository.findByIdForUpdate("acc-a")).thenReturn(Optional.of(from));
        when(accountRepository.findByIdForUpdate("acc-b")).thenReturn(Optional.of(to));

        assertThatThrownBy(() -> paymentService.process(
                new PaymentEvent("p1", "acc-a", "acc-b", new BigDecimal("100.00"), "USD")))
                .isInstanceOf(InsufficientFundsException.class);

        assertThat(payment.getStatus()).isEqualTo(PaymentStatus.FAILED);
        assertThat(from.getBalance()).isEqualByComparingTo("10.00");
        verify(paymentRepository).save(payment);
    }

    @Test
    void processMarksFailedOnUnknownAccount() {
        Payment payment = new Payment("p1", "acc-a", "acc-missing", new BigDecimal("100.00"), "USD");
        Account from = new Account("acc-a", new BigDecimal("500.00"), "USD");

        when(paymentRepository.claimForProcessing("p1")).thenReturn(1);
        when(paymentRepository.findById("p1")).thenReturn(Optional.of(payment));
        when(accountRepository.findByIdForUpdate("acc-a")).thenReturn(Optional.of(from));
        when(accountRepository.findByIdForUpdate("acc-missing")).thenReturn(Optional.empty());

        assertThatThrownBy(() -> paymentService.process(
                new PaymentEvent("p1", "acc-a", "acc-missing", new BigDecimal("100.00"), "USD")))
                .isInstanceOf(AccountNotFoundException.class);

        assertThat(payment.getStatus()).isEqualTo(PaymentStatus.FAILED);
    }

    @Test
    void processMarksFailedOnCurrencyMismatch() {
        Payment payment = new Payment("p1", "acc-a", "acc-b", new BigDecimal("100.00"), "EUR");
        Account from = new Account("acc-a", new BigDecimal("500.00"), "USD");
        Account to = new Account("acc-b", new BigDecimal("0.00"), "USD");

        when(paymentRepository.claimForProcessing("p1")).thenReturn(1);
        when(paymentRepository.findById("p1")).thenReturn(Optional.of(payment));
        when(accountRepository.findByIdForUpdate("acc-a")).thenReturn(Optional.of(from));
        when(accountRepository.findByIdForUpdate("acc-b")).thenReturn(Optional.of(to));

        assertThatThrownBy(() -> paymentService.process(
                new PaymentEvent("p1", "acc-a", "acc-b", new BigDecimal("100.00"), "EUR")))
                .isInstanceOf(CurrencyMismatchException.class);
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

    @Test
    void processRepublishesWithoutRepeatingDebitCreditWhenAlreadyProcessed() {
        Payment payment = new Payment("p1", "acc-a", "acc-b", new BigDecimal("100.00"), "USD");
        payment.markProcessed();

        when(paymentRepository.claimForProcessing("p1")).thenReturn(0);
        when(paymentRepository.findById("p1")).thenReturn(Optional.of(payment));

        PaymentProcessingResult result = paymentService.process(
                new PaymentEvent("p1", "acc-a", "acc-b", new BigDecimal("100.00"), "USD"));

        assertThat(result.outcome()).isEqualTo(PaymentProcessingResult.Outcome.ALREADY_PROCESSED);
        assertThat(result.transactionEvent().paymentId()).isEqualTo("p1");
        verifyNoInteractions(accountRepository);
    }

    @Test
    void createPaymentPersistsInReceivedState() {
        when(paymentRepository.saveAndFlush(any(Payment.class))).thenAnswer(invocation -> invocation.getArgument(0));

        Payment payment = paymentService.createPayment("p1", "acc-a", "acc-b", new BigDecimal("50.00"), "USD");

        assertThat(payment.getStatus()).isEqualTo(PaymentStatus.RECEIVED);
        assertThat(payment.getPaymentId()).isEqualTo("p1");
    }

    @Test
    void createPaymentTranslatesConstraintViolationToDuplicateException() {
        when(paymentRepository.saveAndFlush(any(Payment.class))).thenThrow(new DataIntegrityViolationException("dup"));

        assertThatThrownBy(() -> paymentService.createPayment("p1", "acc-a", "acc-b", new BigDecimal("50.00"), "USD"))
                .isInstanceOf(DuplicatePaymentException.class);
    }
}
