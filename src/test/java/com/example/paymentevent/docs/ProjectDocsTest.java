package com.example.paymentevent.docs;

import com.example.paymentevent.domain.PaymentStatus;
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

    /**
     * The README may only claim what the code does. These five limitations were fixed in
     * Stage 1 (T2 to T6), so the Known limitations section must no longer list them, and must
     * point at what replaced them.
     */
    @Test
    void readmeKnownLimitationsNoLongerListFixedProblems() throws IOException {
        String readme = read("README.md");
        int start = readme.indexOf("## Known limitations");
        int end = readme.indexOf("\n## ", start + 1);
        assertThat(start).as("Known limitations section").isNotNegative();
        String limitations = readme.substring(start, end < 0 ? readme.length() : end);

        assertThat(limitations).doesNotContain(
                "One maximum amount for every currency",
                "Lock timeouts count toward the poison limit",
                "The statement timeout also applies to database migrations",
                "The outbox table grows without limit",
                "`example.com` placeholder");
        assertThat(limitations).contains("max-amount-by-currency", "outbox.retention.days", "lock timeout");
    }

    /**
     * The README's payment status transition table must name every PaymentStatus, so adding a
     * status without documenting how a payment gets into and out of it fails the build.
     */
    @Test
    void readmeTransitionTableCoversEveryPaymentStatus() throws IOException {
        String readme = read("README.md");
        int start = readme.indexOf("### Payment status transitions");
        assertThat(start).as("transition table section").isNotNegative();
        int end = readme.indexOf("\n#", start + 1);
        String table = readme.substring(start, end < 0 ? readme.length() : end);

        for (PaymentStatus status : PaymentStatus.values()) {
            assertThat(table).as("transition table mentions %s", status).contains("`" + status.name() + "`");
        }
    }

    /**
     * S8: the README's screening section covers the topics, the switch, how to run it and
     * what isn't done; every transition is implemented; the roadmap no longer lists the
     * screening hold as future work.
     */
    @Test
    void readmeDocumentsTheScreeningHold() throws IOException {
        String readme = read("README.md");
        int start = readme.indexOf("## Screening hold");
        assertThat(start).as("Screening hold section").isNotNegative();
        int end = readme.indexOf("\n## ", start + 1);
        String screening = readme.substring(start, end < 0 ? readme.length() : end);

        assertThat(screening).contains(
                "### Topics", "`payment.screening.requested`", "`payment.screening.completed`",
                "`payment.screening.completed.DLT`",
                "### Running with screening on", "PAYMENTS_SCREENING_ENABLED", "payments.screening.timeout",
                "### Not done yet");
        assertThat(screening).doesNotContain("not yet (S");

        int roadmap = readme.indexOf("## Roadmap");
        assertThat(roadmap).isNotNegative();
        assertThat(readme.substring(roadmap)).doesNotContain("A fraud check before money moves");
    }

    /**
     * T10: the compose walkthrough was run by hand; the book records it, and the README no
     * longer says it wasn't run and covers CLEAR, FLAG, release and reject.
     */
    @Test
    void screeningWalkthroughWasRunAndRecorded() throws IOException {
        String book = read("docs/PROJECT_BOOK.md");
        int start = book.indexOf("## Demo Walkthrough");
        assertThat(start).as("Demo Walkthrough section in the book").isNotNegative();
        assertThat(book.substring(start)).contains("docker-compose", "kafka-console-producer", "/release", "/reject");

        String readme = read("README.md");
        assertThat(readme).doesNotContain("were not run as part of the verification");
        int run = readme.indexOf("### Running with screening on");
        String walkthrough = readme.substring(run, readme.indexOf("\n### ", run + 1));
        assertThat(walkthrough).contains("\"verdict\":\"CLEAR\"", "\"verdict\":\"FLAG\"", "/release", "/reject");
    }

    private static String read(String path) throws IOException {
        return Files.readString(Path.of(path), StandardCharsets.UTF_8);
    }
}
