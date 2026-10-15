package com.example.paymentevent.security;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The key is required, has no default anywhere in the repo, and is compared by its digest. */
class ApiKeyConfigurationTest {

    @Test
    void applicationYmlTakesTheKeyFromTheEnvironmentWithNoDefault() throws IOException {
        String yml = Files.readString(Path.of("src/main/resources/application.yml"), StandardCharsets.UTF_8);

        assertThat(yml).contains("api-key: \"${PAYMENTS_API_KEY}\"");
        assertThat(yml).doesNotContainPattern("PAYMENTS_API_KEY:[^}]");
    }

    @Test
    void composeFilePassesTheKeyThroughAndRequiresIt() throws IOException {
        String compose = Files.readString(Path.of("docker-compose.yml"), StandardCharsets.UTF_8);

        assertThat(compose).contains("PAYMENTS_API_KEY: ${PAYMENTS_API_KEY:?");
    }

    @Test
    void blankKeyIsRefusedAtStartUp() {
        assertThatThrownBy(() -> new ApiKeyAuthenticationFilter(" "))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("PAYMENTS_API_KEY");
        assertThatThrownBy(() -> new ApiKeyAuthenticationFilter(null))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void onlyTheExactKeyMatches() {
        ApiKeyAuthenticationFilter filter = new ApiKeyAuthenticationFilter("s3cret-key");

        assertThat(filter.matches("s3cret-key")).isTrue();
        assertThat(filter.matches("s3cret-key ")).isFalse();
        assertThat(filter.matches("S3CRET-KEY")).isFalse();
        assertThat(filter.matches("s3cret")).isFalse();
        assertThat(filter.matches("")).isFalse();
        assertThat(filter.matches(null)).isFalse();
    }
}
