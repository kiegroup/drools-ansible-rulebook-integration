package org.drools.ansible.rulebook.integration.loadtests.gen;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.drools.ansible.rulebook.integration.api.io.JsonMapper;

/**
 * Generates the 16 test-event JSON files under src/main/resources/.
 * Deterministic: the only randomness is a fixed-seed word shuffle used to
 * build the bulky message field. Run manually:
 *
 *   mvn -pl drools-ansible-rulebook-integration-load-tests exec:java \
 *       -Dexec.mainClass=org.drools.ansible.rulebook.integration.loadtests.gen.PayloadGenerator \
 *       -Dexec.classpathScope=compile
 */
public final class PayloadGenerator {

    private static final ObjectMapper PRETTY_MAPPER = new ObjectMapper();

    private static final long SEED = 42L;
    private static final int TARGET_EVENT_BYTES = 24_000;

    private static final List<String> WORDS = List.of(
            "request", "response", "latency", "backoff", "retry", "timeout",
            "connection", "pool", "saturated", "drained", "stale", "fresh",
            "cache", "hit", "miss", "eviction", "lease", "expired",
            "cluster", "node", "replica", "leader", "follower", "quorum",
            "partition", "offset", "commit", "rollback", "checkpoint",
            "handler", "listener", "worker", "thread", "queue", "dispatch",
            "event", "payload", "message", "record", "batch", "stream",
            "source", "sink", "topic", "channel", "subscriber", "publisher",
            "rule", "condition", "action", "match", "fire", "evaluate",
            "session", "state", "persistence", "recovery", "failover", "replay",
            "memory", "leak", "allocation", "gc", "heap", "offheap",
            "cpu", "throttle", "saturation", "contention", "lock", "unlock",
            "deadlock", "starvation", "fairness", "priority",
            "disk", "buffer", "flush", "sync", "fsync", "journal",
            "database", "query", "transaction", "isolation", "snapshot",
            "backend", "frontend", "service", "endpoint", "token",
            "authentication", "authorization", "role", "policy",
            "metric", "counter", "gauge", "histogram", "summary", "percentile",
            "trace", "span", "correlation", "context", "propagation",
            "probe", "readiness", "liveness", "health", "ping", "pong",
            "socket", "port", "bind", "listen", "accept", "close",
            "decode", "encode", "serialize", "deserialize", "marshal", "unmarshal",
            "shard", "rebalance", "promote", "demote", "drain", "detach",
            "attach", "mount", "unmount", "rotate", "archive", "purge",
            "apply", "revert", "restore", "migrate", "backfill",
            "scheduler", "trigger", "tick", "cron", "job", "task",
            "observed", "expected", "threshold", "breach", "alarm", "warning",
            "info", "debug", "notice", "critical", "fatal",
            "version", "release", "build", "sha", "branch", "tag",
            "configuration", "parameter", "override", "default", "profile",
            "region", "zone", "namespace", "environment", "tier",
            "quiesce", "resume", "pause", "continue", "abort", "cancel"
    );

    private PayloadGenerator() {}

    public static void main(String[] args) throws IOException {
        Path resourcesDir = Paths.get("drools-ansible-rulebook-integration-load-tests/src/main/resources");
        if (!Files.isDirectory(resourcesDir)) {
            resourcesDir = Paths.get("src/main/resources");
        }
        if (!Files.isDirectory(resourcesDir)) {
            throw new IOException("Cannot locate src/main/resources (run from project root or module root)");
        }

        Map<String, Object> event = buildEvent();

        for (String size : List.of("1k", "5k", "10k", "100k", "1m")) {
            int repeatCount = sizeToRepeatCount(size);
            write(resourcesDir.resolve("24kb_" + size + "_events.json"),
                    matchRuleset("24kb " + size + " events", event, repeatCount));
            write(resourcesDir.resolve("24kb_" + size + "_events_unmatch.json"),
                    unmatchRuleset("24kb " + size + " events unmatch", event, repeatCount));
        }

        for (int n : new int[] { 100, 500, 1000 }) {
            String label = n == 1000 ? "1k" : String.valueOf(n);
            write(resourcesDir.resolve("retention_" + label + "_events.json"),
                    retentionRuleset("retention " + label + " events", event, n));
        }

        for (int n : new int[] { 100, 500, 1000 }) {
            String label = n == 1000 ? "1k" : String.valueOf(n);
            // 10 groups, N/10 iterations → N total events per file.
            int repeatCount = n / 10;
            write(resourcesDir.resolve("once_within_" + label + "_events.json"),
                    temporalRuleset("once_within " + label + " events", event, repeatCount));
        }

        System.out.println("Wrote 16 payload JSON files to " + resourcesDir.toAbsolutePath());
    }

    private static int sizeToRepeatCount(String size) {
        switch (size) {
            case "1k":   return 1_000;
            case "5k":   return 5_000;
            case "10k":  return 10_000;
            case "100k": return 100_000;
            case "1m":   return 1_000_000;
            default: throw new IllegalArgumentException(size);
        }
    }

    private static Map<String, Object> buildEvent() {
        LinkedHashMap<String, Object> event = new LinkedHashMap<>();
        event.put("event_id", "evt-0001");
        event.put("timestamp", "2026-04-21T10:15:30.123Z");

        LinkedHashMap<String, Object> source = new LinkedHashMap<>();
        source.put("host", "host-1");
        source.put("component", "ansible-rulebook");
        source.put("region", "us-east-1");
        source.put("cluster", "cluster-1");
        event.put("source", source);

        event.put("severity", "INFO");
        event.put("tags", List.of("eda", "rulebook", "load-test", "synthetic"));

        LinkedHashMap<String, Object> labels = new LinkedHashMap<>();
        labels.put("env", "prod");
        labels.put("team", "platform");
        labels.put("tier", "critical");
        labels.put("batch", "1");
        event.put("labels", labels);

        LinkedHashMap<String, Object> metrics = new LinkedHashMap<>();
        metrics.put("cpu_pct", 42.7);
        metrics.put("mem_mb", 1337);
        metrics.put("disk_io_mb", 88.2);
        metrics.put("net_rx_kb", 1024);
        metrics.put("net_tx_kb", 2048);
        metrics.put("req_count", 17);
        metrics.put("latency_ms_p50", 12);
        metrics.put("latency_ms_p95", 87);
        metrics.put("latency_ms_p99", 201);
        event.put("metrics", metrics);

        LinkedHashMap<String, Object> trace = new LinkedHashMap<>();
        trace.put("trace_id", "0123456789abcdef0123456789abcdef");
        trace.put("span_id", "fedcba9876543210");
        trace.put("parent_span_id", "89abcdef01234567");
        event.put("trace", trace);

        event.put("message", buildMessage(event));
        event.put("a", 1);
        return event;
    }

    private static String buildMessage(Map<String, Object> eventWithoutMessage) {
        Random rnd = new Random(SEED);
        List<String> shuffled = new ArrayList<>(WORDS);
        Collections.shuffle(shuffled, rnd);

        Map<String, Object> probe = new LinkedHashMap<>(eventWithoutMessage);
        probe.put("message", "");
        probe.put("a", 1);
        int overhead = JsonMapper.toJson(probe).length();
        int targetMessageLen = Math.max(0, TARGET_EVENT_BYTES - overhead);

        StringBuilder sb = new StringBuilder(targetMessageLen + 128);
        int wordIdx = 0;
        int sentenceWordBudget = 0;
        while (sb.length() < targetMessageLen) {
            if (sentenceWordBudget == 0) {
                if (sb.length() > 0) sb.append(". ");
                sentenceWordBudget = 6 + rnd.nextInt(10);
            }
            sb.append(shuffled.get(wordIdx % shuffled.size()));
            wordIdx++;
            sentenceWordBudget--;
            if (sentenceWordBudget > 0) sb.append(' ');
        }
        int lastSpace = sb.lastIndexOf(" ");
        if (lastSpace > 0 && sb.length() - lastSpace < 40) {
            sb.setLength(lastSpace);
        }
        return sb.toString();
    }

    private static Map<String, Object> matchRuleset(String name, Map<String, Object> event, int repeatCount) {
        List<Map<String, Object>> conditions = List.of(equalsCondition("a", 1));
        return buildRuleset(name, event, repeatCount, conditions, /* discardMatchedEvents= */ true);
    }

    private static Map<String, Object> unmatchRuleset(String name, Map<String, Object> event, int repeatCount) {
        List<Map<String, Object>> conditions = List.of(equalsCondition("a", 2));
        return buildRuleset(name, event, repeatCount, conditions, /* discardMatchedEvents= */ true);
    }

    private static Map<String, Object> retentionRuleset(String name, Map<String, Object> event, int repeatCount) {
        List<Map<String, Object>> conditions = List.of(
                equalsCondition("a", 1),
                equalsCondition("b", 1)
        );
        return buildRuleset(name, event, repeatCount, conditions, /* discardMatchedEvents= */ false);
    }

    /**
     * Builds a once_within rule set that groups by a common event field.
     *
     * Shape:
     *   - payload array: 10 entries, one per group_id 0..9
     *   - repeat_count: repeatCount (which will be N/10 for total events = N)
     *   - rule: AllCondition(event.i == 1), action=debug,
     *           throttle { group_by_attributes: ["event.group_id"], once_within: "60 seconds" }
     *   - discard_matched_events: true
     *
     * With 10 groups and first-event-per-group firing, MATCHING_EVENT rows
     * stay at 10 regardless of size; what scales is per-event HA-write and
     * suppression overhead.
     */
    private static Map<String, Object> temporalRuleset(String name, Map<String, Object> event, int repeatCount) {
        List<Map<String, Object>> conditions = List.of(equalsCondition("a", 1));

        LinkedHashMap<String, Object> condition = new LinkedHashMap<>();
        condition.put("AllCondition", conditions);

        LinkedHashMap<String, Object> actionBody = new LinkedHashMap<>();
        actionBody.put("action", "debug");
        actionBody.put("action_args", new LinkedHashMap<>());
        LinkedHashMap<String, Object> action = new LinkedHashMap<>();
        action.put("Action", actionBody);

        LinkedHashMap<String, Object> throttle = new LinkedHashMap<>();
        throttle.put("group_by_attributes", List.of("event.group_id"));
        throttle.put("once_within", "60 seconds");

        LinkedHashMap<String, Object> ruleBody = new LinkedHashMap<>();
        ruleBody.put("name", "r1");
        ruleBody.put("condition", condition);
        ruleBody.put("action", action);
        ruleBody.put("enabled", true);
        ruleBody.put("throttle", throttle);
        LinkedHashMap<String, Object> rule = new LinkedHashMap<>();
        rule.put("Rule", ruleBody);

        // Ten event templates, one per group_id 0..9. Each carries the 24KB
        // realistic payload plus a top-level group_id field.
        List<Map<String, Object>> templates = new ArrayList<>(10);
        for (int g = 0; g < 10; g++) {
            LinkedHashMap<String, Object> copy = new LinkedHashMap<>(event);
            copy.put("group_id", g);
            templates.add(copy);
        }

        LinkedHashMap<String, Object> sourceArgs = new LinkedHashMap<>();
        sourceArgs.put("discard_matched_events", true);
        sourceArgs.put("repeat_count", repeatCount);
        sourceArgs.put("payload", templates);

        LinkedHashMap<String, Object> eventSource = new LinkedHashMap<>();
        eventSource.put("name", "generic");
        eventSource.put("source_name", "generic");
        eventSource.put("source_args", sourceArgs);
        eventSource.put("source_filters", List.of());

        LinkedHashMap<String, Object> sourceEntry = new LinkedHashMap<>();
        sourceEntry.put("EventSource", eventSource);

        LinkedHashMap<String, Object> ruleSet = new LinkedHashMap<>();
        ruleSet.put("name", name);
        ruleSet.put("hosts", List.of("all"));
        ruleSet.put("sources", List.of(sourceEntry));
        ruleSet.put("rules", List.of(rule));

        LinkedHashMap<String, Object> wrapper = new LinkedHashMap<>();
        wrapper.put("RuleSet", ruleSet);
        return wrapper;
    }

    private static Map<String, Object> equalsCondition(String eventField, int value) {
        LinkedHashMap<String, Object> eq = new LinkedHashMap<>();
        LinkedHashMap<String, Object> lhs = new LinkedHashMap<>();
        lhs.put("Event", eventField);
        LinkedHashMap<String, Object> rhs = new LinkedHashMap<>();
        rhs.put("Integer", value);
        eq.put("lhs", lhs);
        eq.put("rhs", rhs);
        LinkedHashMap<String, Object> cond = new LinkedHashMap<>();
        cond.put("EqualsExpression", eq);
        return cond;
    }

    private static Map<String, Object> buildRuleset(String name, Map<String, Object> event, int repeatCount,
                                                    List<Map<String, Object>> conditions, boolean discardMatchedEvents) {
        LinkedHashMap<String, Object> condition = new LinkedHashMap<>();
        condition.put("AllCondition", conditions);

        LinkedHashMap<String, Object> actionBody = new LinkedHashMap<>();
        actionBody.put("action", "debug");
        actionBody.put("action_args", new LinkedHashMap<>());
        LinkedHashMap<String, Object> action = new LinkedHashMap<>();
        action.put("Action", actionBody);

        LinkedHashMap<String, Object> ruleBody = new LinkedHashMap<>();
        ruleBody.put("name", "r1");
        ruleBody.put("condition", condition);
        ruleBody.put("action", action);
        ruleBody.put("enabled", true);
        LinkedHashMap<String, Object> rule = new LinkedHashMap<>();
        rule.put("Rule", ruleBody);

        LinkedHashMap<String, Object> sourceArgs = new LinkedHashMap<>();
        sourceArgs.put("discard_matched_events", discardMatchedEvents);
        sourceArgs.put("repeat_count", repeatCount);
        sourceArgs.put("payload", List.of(event));

        LinkedHashMap<String, Object> eventSource = new LinkedHashMap<>();
        eventSource.put("name", "generic");
        eventSource.put("source_name", "generic");
        eventSource.put("source_args", sourceArgs);
        eventSource.put("source_filters", List.of());

        LinkedHashMap<String, Object> sourceEntry = new LinkedHashMap<>();
        sourceEntry.put("EventSource", eventSource);

        LinkedHashMap<String, Object> ruleSet = new LinkedHashMap<>();
        ruleSet.put("name", name);
        ruleSet.put("hosts", List.of("all"));
        ruleSet.put("sources", List.of(sourceEntry));
        ruleSet.put("rules", List.of(rule));

        LinkedHashMap<String, Object> wrapper = new LinkedHashMap<>();
        wrapper.put("RuleSet", ruleSet);
        return wrapper;
    }

    private static void write(Path path, Map<String, Object> content) throws IOException {
        String json = PRETTY_MAPPER.writerWithDefaultPrettyPrinter()
                .writeValueAsString(List.of(content));
        Files.writeString(path, json + System.lineSeparator(), StandardCharsets.UTF_8);
        System.out.println("  wrote " + path + " (" + json.length() + " bytes)");
    }
}
