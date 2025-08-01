package org.drools.ansible.rulebook.integration.main;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Analyzes memory usage results from load tests to detect potential memory leaks.
 * <p>
 * This analyzer reads test results from a CSV file containing:
 * - Test name (e.g., "24kb_1k_events.json")
 * - Memory usage in bytes
 * - Execution time in milliseconds
 * <p>
 * Analysis Logic:
 * 1. Groups tests by type (matching vs non-matching events)
 * 2. For each group, analyzes memory growth patterns across increasing event counts (1k → 10k → 100k → 1M)
 * 3. Focuses on absolute memory increases rather than ratios to detect acceleration patterns
 * <p>
 * Memory Leak Detection Criteria:
 * <p>
 * 1. Absolute Increase Check:
 * - Flags any single memory increase exceeding 50MB as suspicious
 * <p>
 * 2. Consecutive Acceleration Pattern:
 * - Calculates the ratio between consecutive memory increases
 * - If current increase is >3x the previous increase, it's marked as accelerating
 * - Memory leak is detected only when 2+ consecutive accelerations occur
 * - This prevents false positives from single spikes due to small previous increases
 * <p>
 * 3. Total Memory Growth:
 * - Checks total memory increase from 1k to 1M events
 * - Flags if total increase exceeds 150MB (3x the single-step threshold)
 * <p>
 * Exit Codes:
 * - 0: No memory leak detected
 * - 1: Memory leak detected based on the above criteria
 * - 2: Error during analysis (file parsing, etc.)
 * <p>
 * Usage: java MemoryLeakAnalyzer <result_file>
 */
public class MemoryLeakAnalyzer {

    private static final double INCREASE_GROWTH_THRESHOLD = 3.0; // Max 3x growth in memory increase between steps
    private static final long ABSOLUTE_INCREASE_THRESHOLD = 50_000_000; // 50MB absolute increase warning

    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Usage: java MemoryLeakAnalyzer <result_file>");
            System.exit(1);
        }

        String resultFile = args[0];
        MemoryLeakAnalyzer analyzer = new MemoryLeakAnalyzer();

        try {
            ParseResult parseResult = analyzer.parseResultFile(resultFile);
            boolean hasMemoryLeak = analyzer.analyzeResults(parseResult.results);

            if (hasMemoryLeak || parseResult.exceptionFound) {
                System.err.println("\n❌ MEMORY LEAK DETECTED OR EXCEPTION FOUND!");
                System.err.println("  Review the result file for details.\n");
                System.exit(1);
            } else {
                System.out.println("\n✅ No memory leak detected.");
                System.exit(0);
            }
        } catch (Exception e) {
            System.err.println("Error analyzing results: " + e.getMessage());
            e.printStackTrace();
            System.exit(2);
        }
    }

    private ParseResult parseResultFile(String filename) throws IOException {
        List<TestResult> results = new ArrayList<>();
        boolean exceptionFound = false;

        try (BufferedReader reader = new BufferedReader(new FileReader(filename))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) {
                    continue;
                }

                // Check if this line contains an exception
                if (line.contains("Exception") || line.contains("exception")) {
                    exceptionFound = true;
                }

                // Try to parse as a result line
                String[] parts = line.split(",");
                if (parts.length == 3) {
                    try {
                        String testName = parts[0].trim();
                        long memoryUsage = Long.parseLong(parts[1].trim());
                        long duration = Long.parseLong(parts[2].trim());

                        results.add(new TestResult(testName, memoryUsage, duration));
                    } catch (NumberFormatException e) {
                        // Not a valid result line, skip it
                    }
                }
            }
        }

        if (exceptionFound) {
            System.err.println("\n⚠️  EXCEPTION FOUND IN RESULTS (potentially caused by a memory leak)\n");
        }

        if (results.isEmpty()) {
            throw new IOException("No valid test results found in file. Please check the result file format.");
        }

        return new ParseResult(results, exceptionFound);
    }

    private boolean analyzeResults(List<TestResult> results) {
        boolean hasLeak = false;

        System.out.println("Memory Leak Analysis Report");
        System.out.println("===========================\n");

        // Group results by test type (matching vs unmatching)
        List<TestResult> matchingTests = new ArrayList<>();
        List<TestResult> unmatchingTests = new ArrayList<>();

        for (TestResult result : results) {
            if (result.testName.contains("unmatch")) {
                unmatchingTests.add(result);
            } else {
                matchingTests.add(result);
            }
        }

        System.out.println("Matching Events Tests:");
        hasLeak |= analyzeTestGroup(matchingTests);

        System.out.println("\nNon-Matching Events Tests:");
        hasLeak |= analyzeTestGroup(unmatchingTests);

        return hasLeak;
    }

    private boolean analyzeTestGroup(List<TestResult> tests) {
        if (tests.isEmpty()) {
            return false;
        }

        boolean hasLeak = false;

        // Sort by event count (1k, 10k, 100k, 1m)
        tests.sort((a, b) -> {
            int eventCountA = extractEventCount(a.testName);
            int eventCountB = extractEventCount(b.testName);
            return Integer.compare(eventCountA, eventCountB);
        });

        // Print test results
        System.out.println("Test Name                          Memory (bytes)    Duration (ms)");
        System.out.println("-----------------------------------------------------------------");
        for (TestResult test : tests) {
            System.out.printf("%-34s %,13d    %,12d%n",
                              test.testName, test.memoryUsage, test.duration);
        }

        // Analyze absolute memory increases between consecutive test sizes
        System.out.println("\nMemory Increase Analysis:");
        Long previousIncrease = null;
        int consecutiveIncreases = 0;

        for (int i = 1; i < tests.size(); i++) {
            TestResult prev = tests.get(i - 1);
            TestResult curr = tests.get(i);

            long currentIncrease = curr.memoryUsage - prev.memoryUsage;
            int prevEvents = extractEventCount(prev.testName);
            int currEvents = extractEventCount(curr.testName);

            System.out.printf("  %s → %s:%n",
                              formatEventCount(prevEvents), formatEventCount(currEvents));
            System.out.printf("    Memory increase: %,d bytes", currentIncrease);

            // Special handling for failed tests (0 memory usage typically means the test failed)
            if (curr.memoryUsage == 0) {
                System.out.printf(" ⚠️  TEST FAILED (likely due to memory threshold)!%n");
                hasLeak = true;
            } else if (Math.abs(currentIncrease) > ABSOLUTE_INCREASE_THRESHOLD) {
                System.out.printf(" ⚠️  LARGE INCREASE!%n");
                hasLeak = true;
            } else if (previousIncrease != null && currentIncrease > 0 && previousIncrease > 0) {
                // Compare with previous increase
                double increaseRatio = (double) currentIncrease / previousIncrease;
                System.out.printf(" (%.2fx previous increase)", increaseRatio);

                if (increaseRatio > INCREASE_GROWTH_THRESHOLD) {
                    consecutiveIncreases++;
                    if (consecutiveIncreases >= 2) {
                        System.out.printf(" ⚠️  CONSECUTIVE ACCELERATING GROWTH!%n");
                        hasLeak = true;
                    } else {
                        System.out.printf(" ⚠ (noted, but not a leak if it's not consecutive)%n");
                    }
                } else {
                    consecutiveIncreases = 0; // Reset counter
                    System.out.printf(" ✓%n");
                }
            } else {
                consecutiveIncreases = 0; // Reset counter
                System.out.printf(" ✓%n");
            }

            previousIncrease = currentIncrease;
        }

        // Check total memory increase
        if (tests.size() >= 2) {
            TestResult first = tests.get(0);
            TestResult last = tests.get(tests.size() - 1);
            long totalIncrease = last.memoryUsage - first.memoryUsage;

            System.out.printf("\nTotal memory increase (1k → 1M): %,d bytes", totalIncrease);
            if (totalIncrease > ABSOLUTE_INCREASE_THRESHOLD * 3) {
                System.out.printf(" ⚠️  EXCESSIVE TOTAL INCREASE!%n");
                hasLeak = true;
            } else {
                System.out.printf(" ✓%n");
            }
        }

        return hasLeak;
    }

    private int extractEventCount(String testName) {
        if (testName.contains("1m_")) {
            return 1000000;
        }
        if (testName.contains("100k_")) {
            return 100000;
        }
        if (testName.contains("10k_")) {
            return 10000;
        }
        if (testName.contains("1k_")) {
            return 1000;
        }
        return 0;
    }

    private String formatEventCount(int count) {
        if (count >= 1000000) {
            return (count / 1000000) + "M";
        }
        if (count >= 1000) {
            return (count / 1000) + "k";
        }
        return String.valueOf(count);
    }

    private static class TestResult {

        final String testName;
        final long memoryUsage;
        final long duration;

        TestResult(String testName, long memoryUsage, long duration) {
            this.testName = testName;
            this.memoryUsage = memoryUsage;
            this.duration = duration;
        }
    }

    private static class ParseResult {
        final List<TestResult> results;
        final boolean exceptionFound;

        ParseResult(List<TestResult> results, boolean exceptionFound) {
            this.results = results;
            this.exceptionFound = exceptionFound;
        }
    }
}