package com.example.paymentevent.screening;

import com.example.paymentevent.exception.InvalidScreeningEventException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The verdict consumer's gate: only a payload that passes the S1 schema becomes an event. */
class ScreeningContractsTest {

    private final ScreeningContracts contracts =
            new ScreeningContracts(new ObjectMapper().registerModule(new JavaTimeModule()));

    @Test
    void validVerdictIsParsed() throws IOException {
        String raw = Files.readString(Path.of("contracts/examples/payment.screening.completed.v1.reject.json"));

        ScreeningCompletedEvent event = contracts.parseCompleted(raw);

        assertThat(event.paymentId()).isEqualTo("PAY-20261015-0001");
        assertThat(event.verdict()).isEqualTo(Verdict.REJECT);
        assertThat(event.reasons()).containsExactly("counterparty on sanctions list");
        assertThat(event.riskScore()).isEqualTo(0.98);
        assertThat(event.decidedAt()).isEqualTo(Instant.parse("2026-10-15T09:30:02Z"));
        assertThat(event.screeningRunId()).isEqualTo("run-2026-10-15-0042");
    }

    @Test
    void payloadThatBreaksTheSchemaIsRefusedWithTheReason() throws IOException {
        String raw = Files.readString(Path.of("contracts/examples/payment.screening.completed.v1.clear.json"))
                .replace("\"CLEAR\"", "\"APPROVE\"");

        assertThatThrownBy(() -> contracts.parseCompleted(raw))
                .isInstanceOf(InvalidScreeningEventException.class)
                .hasMessageContaining("verdict");
    }

    @Test
    void payloadThatIsNotJsonIsRefused() {
        assertThatThrownBy(() -> contracts.parseCompleted("{not json"))
                .isInstanceOf(InvalidScreeningEventException.class);
        assertThatThrownBy(() -> contracts.parseCompleted(null))
                .isInstanceOf(InvalidScreeningEventException.class);
    }
}
