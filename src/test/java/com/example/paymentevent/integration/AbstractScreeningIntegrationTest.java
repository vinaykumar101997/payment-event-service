package com.example.paymentevent.integration;

import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

/**
 * Same containers as every other IT, but an application context with screening switched on.
 *
 * Closed after each class (DirtiesContext): while it is open it is a second instance of the
 * service next to the shared screening-off context - same database, same consumer group, its
 * own outbox relay. The service is built to run that way, but tests that inject faults into
 * one context's producer (OutboxIT, PoisonPaymentIT) would have their rows published by the
 * other context's healthy relay if it were still running.
 */
@TestPropertySource(properties = "payments.screening.enabled=true")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public abstract class AbstractScreeningIntegrationTest extends AbstractIntegrationTest {
}
