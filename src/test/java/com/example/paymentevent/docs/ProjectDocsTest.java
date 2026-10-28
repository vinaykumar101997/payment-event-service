package com.example.paymentevent.docs;

import com.example.paymentevent.domain.PaymentStatus;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Keeps the repository's written docs in the shape they promise. Maven runs tests from the
 * project root, so the paths are relative to it.
 */
class ProjectDocsTest {

    /**
     * Words that describe how the work was organised rather than what the code does. Matched
     * case-insensitively on word boundaries. Excluded on purpose: "scheduled task" (a Spring
     * scheduling term) and "status report" (an ISO 20022 pacs.002 message); "task" or "report"
     * inside a longer identifier such as {@code TaskScheduler} never matches a word boundary.
     */
    private static final Pattern PROCESS_WORDS = Pattern.compile(
            "\\bstage[ -]?[12]\\b"
                    + "|(?<!scheduled )\\btasks?\\b"
                    + "|\\binterviews?\\b"
                    + "|\\bQ ?(?:&|and) ?A\\b"
                    + "|(?<!status )\\breports?\\b",
            Pattern.CASE_INSENSITIVE);

    /** Work-item ids such as T0 to T12 and S1 to S8. Case-sensitive, so "t2" in prose is not one. */
    private static final Pattern WORK_ITEM_IDS = Pattern.compile("\\b(?:T(?:1[0-2]|[0-9])|S[1-8])\\b");

    @Test
    void designDocHasAllRequiredSections() throws IOException {
        Path design = Path.of("docs/DESIGN.md");
        assertThat(design).as("docs/DESIGN.md").exists();
        String doc = read(design.toString());

        assertThat(doc).contains(
                "\n## Overview\n",
                "\n## Design decisions\n",
                "\n## Failure cases and fixes\n",
                "\n## Verification\n",
                "\n## Demo walkthrough\n",
                "\n## Known limitations\n");
    }

    @Test
    void readmeLinksToTheDesignDocAndNotToTheOldBook() throws IOException {
        String readme = read("README.md");

        assertThat(readme).contains("](docs/DESIGN.md");
        assertThat(readme).doesNotContainIgnoringCase("PROJECT_BOOK");
    }

    /** Docs describe the service, not the process that built it. */
    @Test
    void docsDoNotTalkAboutHowTheWorkWasOrganised() throws IOException {
        List<Path> files = new ArrayList<>();
        files.add(Path.of("README.md"));
        try (Stream<Path> docs = Files.walk(Path.of("docs"))) {
            docs.filter(f -> f.toString().endsWith(".md")).forEach(files::add);
        }

        List<String> hits = new ArrayList<>();
        for (Path file : files) {
            String[] lines = read(file.toString()).split("\n", -1);
            for (int i = 0; i < lines.length; i++) {
                for (Pattern pattern : List.of(PROCESS_WORDS, WORK_ITEM_IDS)) {
                    Matcher m = pattern.matcher(lines[i]);
                    while (m.find()) {
                        hits.add(file + ":" + (i + 1) + " '" + m.group() + "'");
                    }
                }
            }
        }
        assertThat(hits).as("process words in docs").isEmpty();
    }

    @Test
    void processWordPatternSkipsLegitimateUses() {
        assertThat(PROCESS_WORDS.matcher("runs as a scheduled task every hour").find()).isFalse();
        assertThat(PROCESS_WORDS.matcher("return pacs.002 status reports").find()).isFalse();
        assertThat(PROCESS_WORDS.matcher("uses `TaskScheduler` and `ReportBuilder`").find()).isFalse();
        assertThat(PROCESS_WORDS.matcher("fixed in Stage 2").find()).isTrue();
        assertThat(PROCESS_WORDS.matcher("branch fix/payment-stage1").find()).isTrue();
        assertThat(PROCESS_WORDS.matcher("see the follow-up report").find()).isTrue();
        assertThat(PROCESS_WORDS.matcher("Interview Q&A").find()).isTrue();
        assertThat(WORK_ITEM_IDS.matcher("timeout PT1H, found in T3").find()).isTrue();
        assertThat(WORK_ITEM_IDS.matcher("timeout PT1H").find()).isFalse();
    }

    @Test
    void readmeHasCiBadgeAndVerificationSection() throws IOException {
        String readme = read("README.md");

        assertThat(readme).contains("actions/workflows/ci.yml/badge.svg");
        int start = readme.indexOf("## Verification");
        assertThat(start).as("Verification section").isNotNegative();
        int end = readme.indexOf("\n## ", start + 1);
        String verification = readme.substring(start, end < 0 ? readme.length() : end);

        assertThat(verification).contains("CI runs `./mvnw -B verify`");
        assertThat(verification).containsPattern("\\d+ unit tests and \\d+ integration tests");
    }

    /**
     * The README may only claim what the code does. These five limitations have been fixed, so
     * the Known limitations section must no longer list them, and must point at what replaced
     * them.
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
     * The README's screening section covers the transitions, the topics, the switch and how to
     * run it; Known limitations lists what screening doesn't do yet; the roadmap no longer lists
     * the screening hold as future work.
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
                "### Payment status transitions",
                "### Running with screening on", "PAYMENTS_SCREENING_ENABLED", "payments.screening.timeout");
        assertThat(screening).doesNotContain("not yet (S");

        int limitationsStart = readme.indexOf("## Known limitations");
        assertThat(limitationsStart).as("Known limitations section").isNotNegative();
        int limitationsEnd = readme.indexOf("\n## ", limitationsStart + 1);
        String limitations = readme.substring(limitationsStart,
                limitationsEnd < 0 ? readme.length() : limitationsEnd);
        assertThat(limitations).contains(
                "No screening service here",
                "Overdue is a log line only",
                "Dead-lettered verdicts are not replayed",
                "A lost screening request is not re-sent",
                "Switching screening on doesn't screen older payments",
                "`riskScore` is stored, never used");

        int roadmap = readme.indexOf("## Roadmap");
        assertThat(roadmap).isNotNegative();
        assertThat(readme.substring(roadmap)).doesNotContain("A fraud check before money moves");
    }

    /**
     * The compose walkthrough was run by hand; the design doc holds its commands (CLEAR, FLAG,
     * release and reject) and the recorded transcript, and the README links to it and no longer
     * says it wasn't run.
     */
    @Test
    void screeningWalkthroughWasRunAndRecorded() throws IOException {
        String design = read("docs/DESIGN.md");
        int start = design.indexOf("## Demo walkthrough");
        assertThat(start).as("Demo walkthrough section in docs/DESIGN.md").isNotNegative();
        int end = design.indexOf("\n## ", start + 1);
        String walkthrough = design.substring(start, end < 0 ? design.length() : end);
        assertThat(walkthrough).contains("docker-compose", "kafka-console-producer",
                "\"verdict\":\"CLEAR\"", "\"verdict\":\"FLAG\"", "/release", "/reject");
        assertThat(walkthrough).as("recorded transcript").contains(
                "$ docker-compose up --build -d", "Processed a total of 1 messages",
                "\"status\":\"PROCESSED\"", "\"status\":\"REJECTED\"");

        String readme = read("README.md");
        assertThat(readme).doesNotContain("were not run as part of the verification");
        assertThat(readme).contains("](docs/DESIGN.md#demo-walkthrough)");
    }

    /**
     * Known limitations says the officer name is self-asserted under a shared key, so
     * decisions are not individually authenticated, and what a real system would use.
     */
    @Test
    void readmeKnownLimitationsExplainSelfAssertedOfficers() throws IOException {
        String readme = read("README.md");
        int start = readme.indexOf("## Known limitations");
        int end = readme.indexOf("\n## ", start + 1);
        String limitations = readme.substring(start, end < 0 ? readme.length() : end);

        assertThat(limitations).contains("self-asserted", "not individually authenticated",
                "OpenID Connect", "sub", "four-eyes");
    }

    private static String read(String path) throws IOException {
        return Files.readString(Path.of(path), StandardCharsets.UTF_8);
    }
}
