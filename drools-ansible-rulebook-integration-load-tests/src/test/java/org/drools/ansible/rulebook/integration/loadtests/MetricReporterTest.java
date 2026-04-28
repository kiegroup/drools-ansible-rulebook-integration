package org.drools.ansible.rulebook.integration.loadtests;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MetricReporterTest {

    @Test
    void reportsNoHaLine() {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        try (PrintStream ps = new PrintStream(buf, true, StandardCharsets.UTF_8)) {
            MetricReporter.report(ps, "24kb_1k_events.json", false, false, 5_200_000L, 195L);
        }
        assertThat(buf.toString(StandardCharsets.UTF_8).trim())
                .isEqualTo("24kb_1k_events.json, 5200000, 195");
    }

    @Test
    void reportsHaPgLine() {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        try (PrintStream ps = new PrintStream(buf, true, StandardCharsets.UTF_8)) {
            MetricReporter.report(ps, "24kb_1k_events.json", true, false, 7_100_000L, 240L);
        }
        assertThat(buf.toString(StandardCharsets.UTF_8).trim())
                .isEqualTo("24kb_1k_events.json (HA-PG), 7100000, 240");
    }

    @Test
    void reportsFailoverRecoveryLine() {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        try (PrintStream ps = new PrintStream(buf, true, StandardCharsets.UTF_8)) {
            MetricReporter.report(ps, "retention_100_events.json", true, true, 3_000_000L, 50L);
        }
        assertThat(buf.toString(StandardCharsets.UTF_8).trim())
                .isEqualTo("retention_100_events.json (HA-PG) (failover-recovery), 3000000, 50");
    }
}
