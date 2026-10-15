package com.example.paymentevent.docs;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Keeps the repository's written docs in the shape they promise. Maven runs tests from the
 * project root, so the paths are relative to it.
 */
class ProjectDocsTest {

    @Test
    void projectBookHasAllRequiredSections() throws IOException {
        String book = read("docs/PROJECT_BOOK.md");

        assertThat(book).contains(
                "## 1. Overview and architecture",
                "## 2. Decisions log",
                "## 3. Task Log",
                "## 4. Test and Results Ledger",
                "## 5. Failure stories",
                "## 6. Interview Q and A",
                "## 7. Known gaps and next steps",
                "## 8. Glossary");
        assertThat(book).contains("| Date | Command | Result line | Commit |");
    }

    @Test
    void readmeHasCiBadgeAndVerificationSection() throws IOException {
        String readme = read("README.md");

        assertThat(readme).contains("actions/workflows/ci.yml/badge.svg");
        assertThat(readme).contains("## Verification");
        assertThat(readme).containsPattern("Tests run: \\d+, Failures: 0, Errors: 0");
    }

    private static String read(String path) throws IOException {
        return Files.readString(Path.of(path), StandardCharsets.UTF_8);
    }
}
