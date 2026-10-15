package com.example.paymentevent.screening;

import com.example.paymentevent.domain.Payment;
import com.example.paymentevent.domain.PaymentStatus;
import com.example.paymentevent.exception.UnknownScreenedPaymentException;
import com.example.paymentevent.kafka.PaymentEvent;
import com.example.paymentevent.kafka.PaymentFailedEvent;
import com.example.paymentevent.outbox.OutboxWriter;
import com.example.paymentevent.repository.PaymentRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ScreeningServiceTest {

    @Mock
    private PaymentRepository paymentRepository;

    @Mock
    private ScreeningVerdictRepository verdictRepository;

    @Mock
    private OutboxWriter outboxWriter;

    private ScreeningService screeningService;

    @BeforeEach
    void setUp() {
        screeningService = new ScreeningService(paymentRepository, verdictRepository, outboxWriter);
    }

    @Test
    void clearReleasesAHeldPaymentAndEnqueuesItsPaymentsEvent() {
        Payment payment = heldPayment();
        ScreeningCompletedEvent event = verdict(Verdict.CLEAR);
        when(paymentRepository.findByIdForUpdate("p1")).thenReturn(Optional.of(payment));
        when(verdictRepository.insertIfAbsent(event, VerdictOutcome.RELEASED)).thenReturn(true);

        assertThat(screeningService.applyVerdict(event)).isEqualTo(VerdictOutcome.RELEASED);

        assertThat(payment.getStatus()).isEqualTo(PaymentStatus.RECEIVED);
        verify(outboxWriter).enqueuePaymentEvent(PaymentEvent.from(payment));
        verify(outboxWriter, never()).enqueuePaymentFailedEvent(any());
    }

    @Test
    void rejectRejectsAHeldPaymentAndEnqueuesPaymentFailed() {
        Payment payment = heldPayment();
        ScreeningCompletedEvent event = verdict(Verdict.REJECT);
        when(paymentRepository.findByIdForUpdate("p1")).thenReturn(Optional.of(payment));
        when(verdictRepository.insertIfAbsent(event, VerdictOutcome.REJECTED)).thenReturn(true);

        assertThat(screeningService.applyVerdict(event)).isEqualTo(VerdictOutcome.REJECTED);

        assertThat(payment.getStatus()).isEqualTo(PaymentStatus.REJECTED);
        assertThat(payment.getFailureReason()).isEqualTo("Rejected by screening: counterparty on sanctions list");
        assertThat(payment.getProcessedAt()).isNotNull();
        ArgumentCaptor<PaymentFailedEvent> failed = ArgumentCaptor.forClass(PaymentFailedEvent.class);
        verify(outboxWriter).enqueuePaymentFailedEvent(failed.capture());
        assertThat(failed.getValue().paymentId()).isEqualTo("p1");
        assertThat(failed.getValue().reason()).isEqualTo(payment.getFailureReason());
        verify(outboxWriter, never()).enqueuePaymentEvent(any());
    }

    @ParameterizedTest
    @EnumSource(value = Verdict.class, names = {"FLAG", "ERROR"})
    void flagAndErrorKeepThePaymentHeld(Verdict verdict) {
        Payment payment = heldPayment();
        ScreeningCompletedEvent event = verdict(verdict);
        when(paymentRepository.findByIdForUpdate("p1")).thenReturn(Optional.of(payment));
        when(verdictRepository.insertIfAbsent(event, VerdictOutcome.KEPT_HELD)).thenReturn(true);

        assertThat(screeningService.applyVerdict(event)).isEqualTo(VerdictOutcome.KEPT_HELD);

        assertThat(payment.getStatus()).isEqualTo(PaymentStatus.HELD);
        verifyNoInteractions(outboxWriter);
    }

    @Test
    void duplicateEventIsIgnoredWithoutAnyChange() {
        Payment payment = heldPayment();
        ScreeningCompletedEvent event = verdict(Verdict.CLEAR);
        when(paymentRepository.findByIdForUpdate("p1")).thenReturn(Optional.of(payment));
        when(verdictRepository.insertIfAbsent(event, VerdictOutcome.RELEASED)).thenReturn(false);

        assertThat(screeningService.applyVerdict(event)).isEqualTo(VerdictOutcome.DUPLICATE);

        assertThat(payment.getStatus()).isEqualTo(PaymentStatus.HELD);
        verifyNoInteractions(outboxWriter);
    }

    /** A second, different verdict after the payment left HELD is recorded and ignored. */
    @Test
    void verdictForAPaymentThatIsNoLongerHeldIsRecordedAndIgnored() {
        Payment payment = heldPayment();
        payment.releaseFromHold();
        ScreeningCompletedEvent event = verdict(Verdict.REJECT);
        when(paymentRepository.findByIdForUpdate("p1")).thenReturn(Optional.of(payment));
        when(verdictRepository.insertIfAbsent(event, VerdictOutcome.IGNORED_NOT_HELD)).thenReturn(true);

        assertThat(screeningService.applyVerdict(event)).isEqualTo(VerdictOutcome.IGNORED_NOT_HELD);

        assertThat(payment.getStatus()).isEqualTo(PaymentStatus.RECEIVED);
        verifyNoInteractions(outboxWriter);
    }

    @Test
    void verdictForAnUnknownPaymentIsRefused() {
        when(paymentRepository.findByIdForUpdate("p1")).thenReturn(Optional.empty());

        assertThatThrownBy(() -> screeningService.applyVerdict(verdict(Verdict.CLEAR)))
                .isInstanceOf(UnknownScreenedPaymentException.class);
        verify(verdictRepository, never()).insertIfAbsent(any(), eq(VerdictOutcome.RELEASED));
        verifyNoInteractions(outboxWriter);
    }

    private static Payment heldPayment() {
        Payment payment = new Payment("p1", "acc-a", "acc-b", new BigDecimal("50.00"), "USD");
        payment.holdForScreening();
        return payment;
    }

    private static ScreeningCompletedEvent verdict(Verdict verdict) {
        List<String> reasons = verdict == Verdict.REJECT ? List.of("counterparty on sanctions list") : List.of();
        return new ScreeningCompletedEvent(1, UUID.randomUUID().toString(), "p1", verdict, reasons, 0.5,
                Instant.parse("2026-10-15T09:30:02Z"), "run-1");
    }
}
