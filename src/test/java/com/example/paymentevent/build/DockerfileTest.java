package com.example.paymentevent.build;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The Docker image is built with `mvn package` inside the container, from only what the
 * Dockerfile copies in, and with tests skipped. A resource directory the pom reads but the
 * Dockerfile doesn't copy is silently missing from the image's jar: that is how the image
 * lost contracts/ (the screening schemas) and failed at start-up. Nothing else in the build
 * would notice, so this checks the two files agree.
 */
class DockerfileTest {

    @Test
    void dockerBuildCopiesEveryResourceDirectoryThePomReads() throws IOException {
        String pom = Files.readString(Path.of("pom.xml"), StandardCharsets.UTF_8);
        String dockerfile = Files.readString(Path.of("Dockerfile"), StandardCharsets.UTF_8);

        List<String> resourceDirs = new ArrayList<>();
        Matcher m = Pattern.compile("<resource>\\s*<directory>([^<]+)</directory>").matcher(pom);
        while (m.find()) {
            resourceDirs.add(m.group(1).trim());
        }
        assertThat(resourceDirs).as("resource directories in pom.xml").contains("contracts");

        for (String dir : resourceDirs) {
            String topLevel = dir.split("/")[0];
            assertThat(dockerfile).as("Dockerfile copies %s (needed for resource directory %s)", topLevel, dir)
                    .containsPattern("(?m)^COPY\\s+" + Pattern.quote(topLevel) + "\\s");
        }
    }
}
