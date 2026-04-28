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
 * version-like git tag and fails if the constant is behind the tag.
 *
 * <p>During normal development the constant is bumped to the next target
 * version before the tag is created, so DROOLS_VERSION >= latest tag is
 * expected and passes. The test only fails when DROOLS_VERSION is strictly
 * behind the latest tag (i.e. a tag was created but the constant was not
 * updated).
 *
 * <p>The test is skipped (not failed) when git is unavailable or no
 * version-like tags exist (e.g. shallow CI clone without tags).
 */
class DroolsVersionStalenessTest {

    @Test
    void droolsVersionIsNotBehindLatestGitTag() throws Exception {
        String latestTag = getLatestVersionTag();
        assumeTrue(latestTag != null,
                   "Skipping: no version-like git tags found (shallow clone or tags not fetched)");

        assertThat(compareVersions(AbstractHAStateManager.DROOLS_VERSION, latestTag))
                .as("DROOLS_VERSION is stale! Latest git tag is '%s' but code has '%s'. "
                            + "Update AbstractHAStateManager.DROOLS_VERSION to be >= the latest tag.",
                    latestTag, AbstractHAStateManager.DROOLS_VERSION)
                .isGreaterThanOrEqualTo(0);
    }

    /**
     * Parses the numeric dotted prefix of a version string into an int array.
     */
    private static int[] parseVersion(String version) {
        String numericPart = leadingVersion(version);
        String[] parts = numericPart.split("\\.");
        int[] result = new int[parts.length];
        for (int i = 0; i < parts.length; i++) {
            result[i] = Integer.parseInt(parts[i]);
        }
        return result;
    }

    /**
     * Compares full tag strings after first comparing their numeric dotted prefix.
     * A bare release like {@code 2.0.0} sorts after same-core suffixed variants
     * like {@code 2.0.0-beta1}.
     */
    private static int compareVersions(String a, String b) {
        int[] aParts = parseVersion(a);
        int[] bParts = parseVersion(b);
        int len = Math.max(aParts.length, bParts.length);
        for (int i = 0; i < len; i++) {
            int ai = i < aParts.length ? aParts[i] : 0;
            int bi = i < bParts.length ? bParts[i] : 0;
            if (ai != bi) {
                return ai - bi;
            }
        }
        String aSuffix = suffixAfterVersion(a);
        String bSuffix = suffixAfterVersion(b);
        if (aSuffix.equals(bSuffix)) {
            return 0;
        }
        if (aSuffix.isEmpty()) {
            return 1;
        }
        if (bSuffix.isEmpty()) {
            return -1;
        }
        return aSuffix.compareTo(bSuffix);
    }

    /**
     * Returns the latest version-like git tag, or {@code null} if git is
     * unavailable or no matching tags exist.
     */
    private static String getLatestVersionTag() {
        try {
            Process process = new ProcessBuilder("git", "tag", "-l", "--sort=-v:refname")
                    .redirectErrorStream(true)
                    .start();

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String candidate = line.trim();
                    if (isVersionTag(candidate)) {
                        int exitCode = process.waitFor();
                        if (exitCode == 0) {
                            return candidate;
                        }
                        return null;
                    }
                }
            }

            int exitCode = process.waitFor();
            if (exitCode != 0) {
                return null;
            }
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

    private static String leadingVersion(String version) {
        int start = firstDigitIndex(version);
        if (start < 0) {
            return "";
        }
        StringBuilder builder = new StringBuilder();
        for (int i = start; i < version.length(); i++) {
            char ch = version.charAt(i);
            if (Character.isDigit(ch) || ch == '.') {
                builder.append(ch);
                continue;
            }
            break;
        }
        return builder.toString();
    }

    private static String suffixAfterVersion(String version) {
        String numericPart = leadingVersion(version);
        int start = firstDigitIndex(version);
        if (start < 0) {
            return version;
        }
        return version.substring(start + numericPart.length());
    }

    private static int firstDigitIndex(String value) {
        for (int i = 0; i < value.length(); i++) {
            if (Character.isDigit(value.charAt(i))) {
                return i;
            }
        }
        return -1;
    }
}