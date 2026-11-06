package org.drools.ansible.rulebook.integration.ha.tests;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Verifies that the Maven artifact version is consistent with the latest git tag.
 *
 * <ul>
 *   <li>If the latest tag is a beta (e.g. {@code 2.0.2-beta1}), the artifact version
 *       must be {@code 2.0.2-SNAPSHOT} — the release cycle is still open.</li>
 *   <li>If the latest tag is a final release (e.g. {@code 2.0.2}), the artifact version
 *       must be the next patch SNAPSHOT (e.g. {@code 2.0.3-SNAPSHOT}) — the version
 *       should have been bumped after the release.</li>
 * </ul>
 *
 * <p>Skipped when git is unavailable or no version-like tags exist.
 */
@Disabled("Relying on git command and tag state is brittle. Consider another approach")
class ArtifactVersionConsistencyTest {

    @Test
    void artifactVersionIsConsistentWithLatestTag() throws Exception {
        String projectVersion = loadProjectVersion();
        assumeTrue(projectVersion != null, "Skipping: could not load project version");

        String latestTag = getLatestVersionTag();
        assumeTrue(latestTag != null,
                   "Skipping: no version-like git tags found (shallow clone or tags not fetched)");

        String tagBase = baseVersion(latestTag);
        String tagSuffix = suffixAfterBase(latestTag);

        if (!tagSuffix.isEmpty()) {
            // beta/rc tag (e.g. 2.0.2-beta1) → version should be 2.0.2-SNAPSHOT
            assertThat(projectVersion)
                    .as("Latest tag is '%s' (pre-release), so artifact version should be '%s-SNAPSHOT'",
                        latestTag, tagBase)
                    .isEqualTo(tagBase + "-SNAPSHOT");
        } else {
            // final release tag (e.g. 2.0.2) → version should be next patch SNAPSHOT (e.g. 2.0.3-SNAPSHOT)
            String expectedVersion = nextPatchSnapshot(tagBase);
            assertThat(projectVersion)
                    .as("Latest tag is '%s' (final release), so artifact version should be bumped to '%s'",
                        latestTag, expectedVersion)
                    .isEqualTo(expectedVersion);
        }
    }

    private static String loadProjectVersion() {
        try (InputStream is = ArtifactVersionConsistencyTest.class.getResourceAsStream("/project-version.properties")) {
            if (is == null) {
                return null;
            }
            Properties props = new Properties();
            props.load(is);
            return props.getProperty("project.version");
        } catch (IOException e) {
            return null;
        }
    }

    /**
     * Extracts the numeric dotted prefix (e.g. "2.0.2" from "2.0.2-beta1").
     */
    static String baseVersion(String version) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < version.length(); i++) {
            char ch = version.charAt(i);
            if (Character.isDigit(ch) || ch == '.') {
                sb.append(ch);
            } else {
                break;
            }
        }
        return sb.toString();
    }

    /**
     * Returns everything after the numeric dotted prefix (e.g. "-beta1" from "2.0.2-beta1").
     */
    static String suffixAfterBase(String version) {
        return version.substring(baseVersion(version).length());
    }

    /**
     * Increments the last numeric segment and appends "-SNAPSHOT".
     * e.g. "2.0.2" → "2.0.3-SNAPSHOT"
     */
    static String nextPatchSnapshot(String baseVersion) {
        String[] parts = baseVersion.split("\\.");
        int last = Integer.parseInt(parts[parts.length - 1]);
        parts[parts.length - 1] = String.valueOf(last + 1);
        return String.join(".", parts) + "-SNAPSHOT";
    }

    private static String getLatestVersionTag() {
        try {
            Process process = new ProcessBuilder("git", "-c", "versionsort.suffix=-", "tag", "-l", "--sort=-v:refname")
                    .redirectErrorStream(true)
                    .start();

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String candidate = line.trim();
                    if (isVersionTag(candidate)) {
                        int exitCode = process.waitFor();
                        return exitCode == 0 ? candidate : null;
                    }
                }
            }

            process.waitFor();
            return null;
        } catch (Exception e) {
            return null;
        }
    }

    private static boolean isVersionTag(String tag) {
        return tag != null
                && !tag.isBlank()
                && tag.matches("^\\d+\\.\\d+\\.\\d+(?:-[0-9A-Za-z][0-9A-Za-z.-]*)?$");
    }
}
