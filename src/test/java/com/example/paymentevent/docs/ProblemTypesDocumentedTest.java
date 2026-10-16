package com.example.paymentevent.docs;

import com.example.paymentevent.api.ProblemType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Docs-drift check for problem types. With the default payments.problem-base-uri, a type URI
 * is docs/problems.md#slug, so every slug the API can return must have a heading whose GitHub
 * anchor is exactly that slug, and the page must not document a type no code path returns.
 *
 * "Can return" means a ProblemType constant that ApiExceptionHandler actually uses; an unused
 * constant fails here too, so the enum can't quietly keep a dead type alive in the docs.
 */
class ProblemTypesDocumentedTest {

    private static final Path PROBLEMS_DOC = Path.of("docs/problems.md");
    private static final Path HANDLER_SOURCE =
            Path.of("src/main/java/com/example/paymentevent/api/ApiExceptionHandler.java");

    @Test
    void everyReturnableProblemTypeHasAHeadingAndEveryHeadingIsReturnable() throws IOException {
        Set<String> returnable = returnableSlugs();
        Set<String> documented = documentedAnchors();

        assertThat(returnable).as("problem types with no heading in docs/problems.md")
                .isSubsetOf(documented);
        assertThat(documented).as("headings in docs/problems.md that no code path returns")
                .isSubsetOf(returnable);
    }

    @Test
    void everyProblemTypeConstantIsUsedByTheExceptionHandler() throws IOException {
        String handler = Files.readString(HANDLER_SOURCE, StandardCharsets.UTF_8);

        assertThat(ProblemType.values())
                .as("ProblemType constants ApiExceptionHandler never returns")
                .allSatisfy(type -> assertThat(handler).contains("ProblemType." + type.name()));
    }

    private static Set<String> returnableSlugs() throws IOException {
        String handler = Files.readString(HANDLER_SOURCE, StandardCharsets.UTF_8);
        return Arrays.stream(ProblemType.values())
                .filter(type -> handler.contains("ProblemType." + type.name()))
                .map(ProblemType::slug)
                .collect(Collectors.toSet());
    }

    /** Level-2 headings are problem types; the page title (level 1) is not. */
    private static Set<String> documentedAnchors() throws IOException {
        return Files.readAllLines(PROBLEMS_DOC, StandardCharsets.UTF_8).stream()
                .filter(line -> line.startsWith("## "))
                .map(line -> githubAnchor(line.substring(3)))
                .collect(Collectors.toSet());
    }

    /**
     * GitHub's heading anchor: lower-cased, punctuation other than hyphens and underscores
     * dropped, spaces turned into hyphens.
     */
    static String githubAnchor(String heading) {
        return heading.trim().toLowerCase(Locale.ROOT)
                .replaceAll("[^\\p{L}\\p{N} _-]", "")
                .replace(' ', '-');
    }
}
