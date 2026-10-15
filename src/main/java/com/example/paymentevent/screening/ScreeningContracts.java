package com.example.paymentevent.screening;

import com.example.paymentevent.exception.InvalidScreeningEventException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.Error;
import com.networknt.schema.Schema;
import com.networknt.schema.SchemaRegistry;
import com.networknt.schema.SchemaRegistryConfig;
import com.networknt.schema.SpecificationVersion;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Validates screening payloads against the JSON Schemas in contracts/ (copied onto the
 * classpath by the build), the same files the screening service is built against. A payload
 * becomes a ScreeningCompletedEvent only if it validates; otherwise InvalidScreeningEventException,
 * which the verdict listener's error handler sends to the DLT without retrying.
 *
 * Format assertions (date-time, uuid) are switched on: in draft 2020-12 they are annotations
 * only by default, and "decidedAt": "yesterday" must not pass.
 */
@Component
public class ScreeningContracts {

    static final String COMPLETED_V1 = "/contracts/payment.screening.completed.v1.schema.json";
    private static final int MAX_ERRORS_REPORTED = 5;

    private final ObjectMapper objectMapper;
    private final Schema completedSchema;

    public ScreeningContracts(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        SchemaRegistry registry = SchemaRegistry.withDefaultDialect(SpecificationVersion.DRAFT_2020_12,
                builder -> builder.schemaRegistryConfig(
                        SchemaRegistryConfig.builder().formatAssertionsEnabled(true).build()));
        this.completedSchema = load(registry, COMPLETED_V1);
    }

    public ScreeningCompletedEvent parseCompleted(String raw) {
        if (raw == null || raw.isBlank()) {
            throw new InvalidScreeningEventException("Empty payment.screening.completed payload");
        }
        JsonNode node;
        try {
            node = objectMapper.readTree(raw);
        } catch (JsonProcessingException e) {
            throw new InvalidScreeningEventException("payment.screening.completed payload is not JSON: "
                    + e.getOriginalMessage(), e);
        }
        List<Error> errors = completedSchema.validate(node);
        if (!errors.isEmpty()) {
            throw new InvalidScreeningEventException("payment.screening.completed payload does not match the v1 schema: "
                    + errors.stream().limit(MAX_ERRORS_REPORTED).map(Error::toString).collect(Collectors.joining("; ")));
        }
        try {
            return objectMapper.treeToValue(node, ScreeningCompletedEvent.class);
        } catch (JsonProcessingException e) {
            throw new InvalidScreeningEventException("Valid payment.screening.completed payload could not be mapped: "
                    + e.getOriginalMessage(), e);
        }
    }

    private static Schema load(SchemaRegistry registry, String resource) {
        try (InputStream in = ScreeningContracts.class.getResourceAsStream(resource)) {
            if (in == null) {
                throw new IllegalStateException("Schema " + resource + " is not on the classpath");
            }
            return registry.getSchema(in);
        } catch (IOException e) {
            throw new UncheckedIOException("Could not read schema " + resource, e);
        }
    }
}
