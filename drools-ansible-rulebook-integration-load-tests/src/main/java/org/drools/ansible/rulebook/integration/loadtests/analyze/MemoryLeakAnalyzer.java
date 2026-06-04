package org.drools.ansible.rulebook.integration.loadtests.analyze;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * See spec section 5.8. Groups test results four ways
 * (match|unmatch) x (noHA|HA-PG), sorts each group by event count, applies the
 * same absolute-increase / consecutive-acceleration / total-increase thresholds
 * used in the main module, and flags a leak if any group trips a threshold.
 */
public class MemoryLeakAnalyzer {

    private static final double INCREASE_GROWTH_THRESHOLD = 3.0;
    private static final long ABSOLUTE_INCREASE_THRESHOLD = 50_000_000;
    private static final double TIME_PER_EVENT_GROWTH_THRESHOLD = 2.5;
    private static final double TOTAL_TIME_PER_EVENT_GROWTH_THRESHOLD = 4.0;

    public static void main(String[] args) {
        CliOptions options = CliOptions.parse(args);
        if (options == null) {
            System.err.println("Usage: java MemoryLeakAnalyzer [--ignore-time-anomaly-group=<group>] <result_file>");
            System.exit(1);
        }
        try {
            AnalyzeResult r = new MemoryLeakAnalyzer().analyzeFile(options.resultFile, options.ignoredTimeAnomalyGroups);
            if (r.hasLeak || r.hasTimeAnomaly || r.exceptionFound) {
                System.err.println("\n❌ MEMORY LEAK, RESPONSE-TIME ANOMALY, OR EXCEPTION FOUND!");
                System.err.println("  Review the result file for details.\n");
                System.exit(1);
            }
            System.out.println("\n✅ No memory leak or response-time anomaly detected.");
            System.exit(0);
        } catch (Exception e) {
            System.err.println("Error analyzing results: " + e.getMessage());
            e.printStackTrace();
            System.exit(2);
        }
    }

    public AnalyzeResult analyzeFile(String filename) throws IOException {
        return analyzeFile(filename, Set.of());
    }

    public AnalyzeResult analyzeFile(String filename, Set<String> ignoredTimeAnomalyGroups) throws IOException {
        ParseResult pr = parseResultFile(filename);
        AnalysisSummary summary = analyzeResults(pr.results, ignoredTimeAnomalyGroups);
        return new AnalyzeResult(summary.hasLeak, summary.hasTimeAnomaly, pr.exceptionFound);
    }

    private ParseResult parseResultFile(String filename) throws IOException {
        List<TestResult> results = new ArrayList<>();
        boolean exceptionFound = false;

        try (BufferedReader reader = new BufferedReader(new FileReader(filename))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;
                if (line.contains("Exception") || line.contains("exception")) {
                    exceptionFound = true;
                }
                String[] parts = line.split(",");
                if (parts.length == 3) {
                    try {
                        String testName = parts[0].trim();
                        long mem = Long.parseLong(parts[1].trim());
                        long dur = Long.parseLong(parts[2].trim());
                        results.add(new TestResult(testName, mem, dur));
                    } catch (NumberFormatException ignored) {
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

    private AnalysisSummary analyzeResults(List<TestResult> results, Set<String> ignoredTimeAnomalyGroups) {
        System.out.println("Memory Leak Analysis Report");
        System.out.println("===========================\n");

        Map<String, List<TestResult>> groups = new LinkedHashMap<>();
        for (TestResult r : results) {
            groups.computeIfAbsent(groupKey(r.testName), k -> new ArrayList<>()).add(r);
        }

        boolean hasLeak = false;
        boolean hasTimeAnomaly = false;
        for (Map.Entry<String, List<TestResult>> entry : groups.entrySet()) {
            List<TestResult> tests = entry.getValue();
            if (tests.isEmpty()) continue;
            String groupKey = entry.getKey();
            System.out.println(groupKey + ":");
            GroupAnalysis analysis = analyzeTestGroup(groupKey, tests, ignoredTimeAnomalyGroups.contains(groupKey));
            hasLeak |= analysis.hasLeak;
            hasTimeAnomaly |= analysis.hasTimeAnomaly;
            System.out.println();
        }
        return new AnalysisSummary(hasLeak, hasTimeAnomaly);
    }

    private GroupAnalysis analyzeTestGroup(String groupKey, List<TestResult> tests, boolean ignoreTimeAnomaly) {
        if (tests.isEmpty()) return new GroupAnalysis(false, false);

        tests.sort((a, b) -> Integer.compare(extractEventCount(a.testName), extractEventCount(b.testName)));

        System.out.println("Test Name                                 Memory (bytes)    Duration (ms)");
        System.out.println("------------------------------------------------------------------------");
        for (TestResult t : tests) {
            System.out.printf("%-41s %,13d    %,12d%n", t.testName, t.memoryUsage, t.duration);
        }

        System.out.println("\nMemory Increase Analysis:");
        boolean hasLeak = false;
        Long previousIncrease = null;
        int consecutive = 0;

        for (int i = 1; i < tests.size(); i++) {
            TestResult prev = tests.get(i - 1);
            TestResult curr = tests.get(i);
            long increase = curr.memoryUsage - prev.memoryUsage;

            System.out.printf("  %s → %s:%n",
                    formatEventCount(extractEventCount(prev.testName)),
                    formatEventCount(extractEventCount(curr.testName)));
            System.out.printf("    Memory increase: %,d bytes", increase);

            if (curr.memoryUsage == 0) {
                System.out.printf(" ⚠️  TEST FAILED (likely due to memory threshold)!%n");
                hasLeak = true;
            } else if (Math.abs(increase) > ABSOLUTE_INCREASE_THRESHOLD) {
                System.out.printf(" ⚠️  LARGE INCREASE!%n");
                hasLeak = true;
            } else if (previousIncrease != null && increase > 0 && previousIncrease > 0) {
                double ratio = (double) increase / previousIncrease;
                System.out.printf(" (%.2fx previous increase)", ratio);
                if (ratio > INCREASE_GROWTH_THRESHOLD) {
                    consecutive++;
                    if (consecutive >= 2) {
                        System.out.printf(" ⚠️  CONSECUTIVE ACCELERATING GROWTH!%n");
                        hasLeak = true;
                    } else {
                        System.out.printf(" ⚠ (noted, but not a leak if it's not consecutive)%n");
                    }
                } else {
                    consecutive = 0;
                    System.out.printf(" ✓%n");
                }
            } else {
                consecutive = 0;
                System.out.printf(" ✓%n");
            }
            previousIncrease = increase;
        }

        if (tests.size() >= 2) {
            long total = tests.get(tests.size() - 1).memoryUsage - tests.get(0).memoryUsage;
            System.out.printf("\nTotal memory increase (first → last): %,d bytes", total);
            if (total > ABSOLUTE_INCREASE_THRESHOLD * 3) {
                System.out.printf(" ⚠️  EXCESSIVE TOTAL INCREASE!%n");
                hasLeak = true;
            } else {
                System.out.printf(" ✓%n");
            }
        }

        System.out.println("\nResponse Time Scaling Analysis:");
        boolean hasTimeAnomaly = false;
        int consecutiveTimeAnomalies = 0;
        Double previousPerEventTime = null;
        TestResult first = tests.get(0);
        TestResult last = tests.get(tests.size() - 1);
        for (TestResult curr : tests) {
            int events = extractEventCount(curr.testName);
            if (events <= 0 || curr.duration <= 0) {
                continue;
            }
            double perEventTime = (double) curr.duration / events;
            if (previousPerEventTime != null) {
                double ratio = perEventTime / previousPerEventTime;
                System.out.printf("  %s: %.6f ms/event (%.2fx previous)%n",
                        curr.testName, perEventTime, ratio);
                if (ratio > TIME_PER_EVENT_GROWTH_THRESHOLD) {
                    consecutiveTimeAnomalies++;
                    if (consecutiveTimeAnomalies >= 2) {
                        System.out.println("    ⚠️  SUPERLINEAR RESPONSE-TIME GROWTH!");
                        hasTimeAnomaly = true;
                    } else {
                        System.out.println("    ⚠ first superlinear increase noted");
                    }
                } else {
                    consecutiveTimeAnomalies = 0;
                }
            } else {
                System.out.printf("  %s: %.6f ms/event%n", curr.testName, perEventTime);
            }
            previousPerEventTime = perEventTime;
        }

        int firstEvents = extractEventCount(first.testName);
        int lastEvents = extractEventCount(last.testName);
        if (firstEvents > 0 && lastEvents > 0 && first.duration > 0 && last.duration > 0 && tests.size() >= 2) {
            double firstPerEvent = (double) first.duration / firstEvents;
            double lastPerEvent = (double) last.duration / lastEvents;
            double totalRatio = lastPerEvent / firstPerEvent;
            System.out.printf("%nTotal per-event time change (first → last): %.2fx", totalRatio);
            if (totalRatio > TOTAL_TIME_PER_EVENT_GROWTH_THRESHOLD) {
                System.out.printf(" ⚠️  EXCESSIVE SUPERLINEAR GROWTH!%n");
                hasTimeAnomaly = true;
            } else {
                System.out.printf(" ✓%n");
            }
        }
        if (ignoreTimeAnomaly && hasTimeAnomaly) {
            System.out.printf("  NOTE: response-time anomaly for group '%s' is configured as warn-only and will not fail the analyzer. Tracked in issue #183: https://github.com/kiegroup/drools-ansible-rulebook-integration/issues/183%n",
                    groupKey);
            hasTimeAnomaly = false;
        }
        return new GroupAnalysis(hasLeak, hasTimeAnomaly);
    }

    private String groupKey(String testName) {
        String scenario;
        if (testName.contains("failover-recovery")) {
            scenario = "failover-recovery";
        } else if (testName.contains("unmatch")) {
            scenario = "unmatch";
        } else if (testName.contains("retention_")) {
            scenario = "retention";
        } else if (testName.contains("once_within_")) {
            scenario = "temporal";
        } else {
            scenario = "match";
        }
        String mode = testName.contains(" (HA-PG)") ? "HA-PG" : "noHA";
        return scenario + "/" + mode;
    }

    private int extractEventCount(String name) {
        if (name.contains("1m_")) return 1_000_000;
        if (name.contains("100k_")) return 100_000;
        if (name.contains("10k_")) return 10_000;
        if (name.contains("5k_")) return 5_000;
        if (name.contains("1k_")) return 1_000;
        if (name.contains("500_events")) return 500;
        if (name.contains("100_events")) return 100;
        return 0;
    }

    private String formatEventCount(int c) {
        if (c >= 1_000_000) return (c / 1_000_000) + "M";
        if (c >= 1_000) return (c / 1_000) + "k";
        return String.valueOf(c);
    }

    public static final class AnalyzeResult {
        public final boolean hasLeak;
        public final boolean hasTimeAnomaly;
        public final boolean exceptionFound;

        public AnalyzeResult(boolean hasLeak, boolean hasTimeAnomaly, boolean exceptionFound) {
            this.hasLeak = hasLeak;
            this.hasTimeAnomaly = hasTimeAnomaly;
            this.exceptionFound = exceptionFound;
        }
    }

    private static final class AnalysisSummary {
        final boolean hasLeak;
        final boolean hasTimeAnomaly;

        private AnalysisSummary(boolean hasLeak, boolean hasTimeAnomaly) {
            this.hasLeak = hasLeak;
            this.hasTimeAnomaly = hasTimeAnomaly;
        }
    }

    private static final class GroupAnalysis {
        final boolean hasLeak;
        final boolean hasTimeAnomaly;

        private GroupAnalysis(boolean hasLeak, boolean hasTimeAnomaly) {
            this.hasLeak = hasLeak;
            this.hasTimeAnomaly = hasTimeAnomaly;
        }
    }

    private static final class CliOptions {
        final String resultFile;
        final Set<String> ignoredTimeAnomalyGroups;

        private CliOptions(String resultFile, Set<String> ignoredTimeAnomalyGroups) {
            this.resultFile = resultFile;
            this.ignoredTimeAnomalyGroups = ignoredTimeAnomalyGroups;
        }

        static CliOptions parse(String[] args) {
            if (args.length == 0) {
                return null;
            }
            String resultFile = null;
            Set<String> ignoredGroups = new HashSet<>();
            for (String arg : args) {
                if (arg.startsWith("--ignore-time-anomaly-group=")) {
                    ignoredGroups.add(arg.substring("--ignore-time-anomaly-group=".length()));
                } else if (resultFile == null) {
                    resultFile = arg;
                } else {
                    return null;
                }
            }
            if (resultFile == null) {
                return null;
            }
            return new CliOptions(resultFile, ignoredGroups);
        }
    }

    private static final class TestResult {
        final String testName;
        final long memoryUsage;
        final long duration;

        TestResult(String testName, long memoryUsage, long duration) {
            this.testName = testName;
            this.memoryUsage = memoryUsage;
            this.duration = duration;
        }
    }

    private static final class ParseResult {
        final List<TestResult> results;
        final boolean exceptionFound;

        ParseResult(List<TestResult> results, boolean exceptionFound) {
            this.results = results;
            this.exceptionFound = exceptionFound;
        }
    }
}
