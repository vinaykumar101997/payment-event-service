package com.example.paymentevent.kafka;

import com.example.paymentevent.exception.InsufficientFundsException;
import com.example.paymentevent.exception.PaymentEventMismatchException;
import com.example.paymentevent.exception.PaymentPoisonedException;
import com.example.paymentevent.service.PaymentService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PaymentConsumerTest {

    private static final PaymentEvent EVENT =
            new PaymentEvent("p1", "acc-a", "acc-b", new BigDecimal("100.00"), "USD");

    @Mock
    private PaymentService paymentService;

    private PaymentConsumer paymentConsumer;

    @BeforeEach
    void setUp() {
        paymentConsumer = new PaymentConsumer(paymentService);
    }

    @Test
    void nonBusinessFailureIsCountedAndRethrownSoKafkaRetriesIt() {
        RuntimeException failure = new IllegalStateException("database unavailable");
        when(paymentService.process(EVENT)).thenThrow(failure);
        when(paymentService.recordFailedAttempt("p1", failure)).thenReturn(false);

        assertThatThrownBy(() -> paymentConsumer.onPaymentEvent(EVENT)).isSameAs(failure);

        verify(paymentService).recordFailedAttempt("p1", failure);
    }

    @Test
    void failureThatExhaustsTheAttemptLimitBecomesNonRetryablePoisonedException() {
        RuntimeException failure = new IllegalStateException("database unavailable");
        when(paymentService.process(EVENT)).thenThrow(failure);
        when(paymentService.recordFailedAttempt("p1", failure)).thenReturn(true);

        assertThatThrownBy(() -> paymentConsumer.onPaymentEvent(EVENT))
                .isInstanceOf(PaymentPoisonedException.class)
                .hasCause(failure);
    }

    @Test
    void businessAndMismatchFailuresAreNotCountedAsAttempts() {
        when(paymentService.process(EVENT)).thenThrow(new PaymentEventMismatchException("amount differs"));

        assertThatThrownBy(() -> paymentConsumer.onPaymentEvent(EVENT))
                .isInstanceOf(PaymentEventMismatchException.class);

        verify(paymentService, never()).recordFailedAttempt(any(), any());
    }

    @Test
    void insufficientFundsIsNotCountedAsAnAttempt() {
        when(paymentService.process(EVENT)).thenThrow(new InsufficientFundsException("no funds"));

        assertThatThrownBy(() -> paymentConsumer.onPaymentEvent(EVENT))
                .isInstanceOf(InsufficientFundsException.class);

        verify(paymentService, never()).recordFailedAttempt(any(), any());
    }

    /** Recording the attempt needs the DB too; if that fails, the original error still wins. */
    @Test
    void failureToRecordTheAttemptDoesNotHideTheOriginalError() {
        RuntimeException failure = new IllegalStateException("database unavailable");
        when(paymentService.process(EVENT)).thenThrow(failure);
        when(paymentService.recordFailedAttempt("p1", failure)).thenThrow(new IllegalStateException("still down"));

        assertThatThrownBy(() -> paymentConsumer.onPaymentEvent(EVENT)).isSameAs(failure);
    }
}
