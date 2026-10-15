package com.example.paymentevent.contracts;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.networknt.schema.Error;
import com.networknt.schema.Schema;
import com.networknt.schema.SchemaRegistry;
import com.networknt.schema.SchemaRegistryConfig;
import com.networknt.schema.SpecificationVersion;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The screening contracts in contracts/ are shared with the Python screening service. These
 * tests pin them: the sample events in contracts/examples/ must validate, and each listed
 * break of the contract must be rejected. The schemas are read from the classpath (the build
 * copies contracts/ there), which is also where the verdict consumer loads them from.
 */
class ScreeningContractsTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final SchemaRegistry REGISTRY = SchemaRegistry.withDefaultDialect(SpecificationVersion.DRAFT_2020_12,
            builder -> builder.schemaRegistryConfig(SchemaRegistryConfig.builder().formatAssertionsEnabled(true).build()));

    private static final String REQUESTED = "payment.screening.requested.v1";
    private static final String COMPLETED = "payment.screening.completed.v1";

    @Test
    void schemasDeclareDraft2020_12() throws IOException {
        for (String name : List.of(REQUESTED, COMPLETED)) {
            JsonNode schema = MAPPER.readTree(Files.readString(Path.of("contracts", name + ".schema.json")));
            assertThat(schema.path("$schema").asText()).as(name)
                    .isEqualTo("https://json-schema.org/draft/2020-12/schema");
        }
    }

    @Test
    void requestedExampleIsValid() throws IOException {
        assertThat(validate(REQUESTED, example(REQUESTED))).isEmpty();
    }

    @ParameterizedTest
    @MethodSource("completedExamples")
    void completedExamplesAreValid(String file) throws IOException {
        assertThat(validate(COMPLETED, read(Path.of("contracts", "examples", file)))).isEmpty();
    }

    static Stream<String> completedExamples() {
        return Stream.of(COMPLETED + ".clear.json", COMPLETED + ".flag.json",
                COMPLETED + ".reject.json", COMPLETED + ".error.json");
    }

    @Test
    void requestedRejectsContractBreaks() throws IOException {
        assertInvalid(REQUESTED, e -> e.remove("eventId"));
        assertInvalid(REQUESTED, e -> e.remove("createdAt"));
        assertInvalid(REQUESTED, e -> e.put("amount", 10.5));
        assertInvalid(REQUESTED, e -> e.put("amount", "-10.00"));
        assertInvalid(REQUESTED, e -> e.put("amount", "1e3"));
        assertInvalid(REQUESTED, e -> e.put("amount", "0.00"));
        assertInvalid(REQUESTED, e -> e.put("amount", "10.123"));
        assertInvalid(REQUESTED, e -> e.put("currency", "usd"));
        assertInvalid(REQUESTED, e -> e.put("schemaVersion", 2));
        assertInvalid(REQUESTED, e -> e.put("createdAt", "yesterday"));
        assertInvalid(REQUESTED, e -> e.put("eventId", "not-a-uuid"));
        assertInvalid(REQUESTED, e -> e.put("unexpected", "x"));
    }

    @Test
    void completedRejectsContractBreaks() throws IOException {
        assertInvalid(COMPLETED, e -> e.put("verdict", "APPROVE"));
        assertInvalid(COMPLETED, e -> e.remove("verdict"));
        assertInvalid(COMPLETED, e -> e.remove("reasons"));
        assertInvalid(COMPLETED, e -> e.putArray("reasons").add(1));
        assertInvalid(COMPLETED, e -> e.put("riskScore", "0.9"));
        assertInvalid(COMPLETED, e -> e.remove("screeningRunId"));
        assertInvalid(COMPLETED, e -> e.put("decidedAt", "2026-13-40T00:00:00Z"));
        assertInvalid(COMPLETED, e -> e.remove("paymentId"));
        assertInvalid(COMPLETED, e -> e.put("unexpected", "x"));
    }

    @Test
    void riskScoreIsOptional() throws IOException {
        ObjectNode event = (ObjectNode) example(COMPLETED);
        event.remove("riskScore");
        assertThat(validate(COMPLETED, event)).isEmpty();
    }

    private static void assertInvalid(String contract, Consumer<ObjectNode> breakIt) throws IOException {
        ObjectNode event = (ObjectNode) example(contract);
        breakIt.accept(event);
        assertThat(validate(contract, event)).as("%s should reject %s", contract, event).isNotEmpty();
    }

    private static JsonNode example(String contract) throws IOException {
        String file = contract.equals(COMPLETED) ? COMPLETED + ".clear.json" : contract + ".json";
        return read(Path.of("contracts", "examples", file));
    }

    private static JsonNode read(Path path) throws IOException {
        return MAPPER.readTree(Files.readString(path));
    }

    private static List<Error> validate(String contract, JsonNode event) throws IOException {
        try (InputStream in = ScreeningContractsTest.class.getResourceAsStream("/contracts/" + contract + ".schema.json")) {
            assertThat(in).as("schema %s on the classpath", contract).isNotNull();
            Schema schema = REGISTRY.getSchema(in);
            return schema.validate(event);
        }
    }
}
