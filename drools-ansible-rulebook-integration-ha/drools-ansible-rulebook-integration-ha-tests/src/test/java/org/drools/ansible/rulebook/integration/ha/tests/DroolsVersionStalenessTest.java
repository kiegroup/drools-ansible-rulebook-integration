package org.drools.ansible.rulebook.integration.ha.tests;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.drools.ansible.rulebook.integration.ha.api.AbstractHAStateManager;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Guards against a stale DROOLS_VERSION constant.
 * Compares {@link AbstractHAStateManager#DROOLS_VERSION} with the latest
 * {@code ha-poc-*} git tag and fails if the constant is behind the tag.
 *
 * <p>During normal development the constant is bumped to the next target
 * version before the tag is created, so DROOLS_VERSION >= latest tag is
 * expected and passes. The test only fails when DROOLS_VERSION is strictly
 * behind the latest tag (i.e. a tag was created but the constant was not
 * updated).
 *
 * <p>The test is skipped (not failed) when git is unavailable or no
 * {@code ha-poc-*} tags exist (e.g. shallow CI clone without tags).
 */
class DroolsVersionStalenessTest {

    private static final String TAG_PREFIX = "ha-poc-";

    @Test
    void droolsVersionIsNotBehindLatestGitTag() throws Exception {
        String latestTag = getLatestHaPocTag();
        assumeTrue(latestTag != null,
                "Skipping: no ha-poc-* git tags found (shallow clone or tags not fetched)");

        int[] tagVersion = parseVersion(latestTag);
        int[] codeVersion = parseVersion(AbstractHAStateManager.DROOLS_VERSION);

        assertThat(compareVersions(codeVersion, tagVersion))
                .as("DROOLS_VERSION is stale! Latest git tag is '%s' but code has '%s'. "
                    + "Update AbstractHAStateManager.DROOLS_VERSION to be >= the latest tag.",
                    latestTag, AbstractHAStateManager.DROOLS_VERSION)
                .isGreaterThanOrEqualTo(0);
    }

    /**
     * Parses "ha-poc-X.Y.Z" into {@code [X, Y, Z]}.
     */
    private static int[] parseVersion(String version) {
        String numericPart = version.startsWith(TAG_PREFIX)
                ? version.substring(TAG_PREFIX.length())
                : version;
        String[] parts = numericPart.split("\\.");
        int[] result = new int[parts.length];
        for (int i = 0; i < parts.length; i++) {
            result[i] = Integer.parseInt(parts[i]);
        }
        return result;
    }

    /**
     * Standard semantic version comparison. Returns positive if a > b,
     * negative if a < b, zero if equal.
     */
    private static int compareVersions(int[] a, int[] b) {
        int len = Math.max(a.length, b.length);
        for (int i = 0; i < len; i++) {
            int ai = i < a.length ? a[i] : 0;
            int bi = i < b.length ? b[i] : 0;
            if (ai != bi) {
                return ai - bi;
            }
        }
        return 0;
    }

    /**
     * Returns the latest {@code ha-poc-*} git tag sorted by version, or {@code null}
     * if git is unavailable or no matching tags exist.
     */
    private static String getLatestHaPocTag() {
        try {
            Process process = new ProcessBuilder("git", "tag", "-l", "ha-poc-*", "--sort=-v:refname")
                    .redirectErrorStream(true)
                    .start();

            String firstLine;
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                firstLine = reader.readLine();
            }

            int exitCode = process.waitFor();
            if (exitCode != 0 || firstLine == null || firstLine.isBlank()) {
                return null;
            }
            return firstLine.trim();
        } catch (Exception e) {
            return null;
        }
    }
}
