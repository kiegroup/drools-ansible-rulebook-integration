package org.drools.ansible.rulebook.integration.loadtests;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MeasurementTest {

    @Test
    void timeWork_returnsMatchesAndNonNegativeDuration() {
        List<Map> stubMatches = List.of(Map.of("k", "v"));
        Measurement.TimedResult t = Measurement.timeWork(() -> {
            // simulate a little work
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return new Payload.Execution(stubMatches, stubMatches.size());
        });
        assertThat(t.matches).isSameAs(stubMatches);
        assertThat(t.matchCount).isEqualTo(1);
        assertThat(t.durationMs).isGreaterThanOrEqualTo(0L);
    }

    @Test
    void captureUsedMemoryAfterGc_returnsPositive() {
        // After running System.gc(), used = totalMemory - freeMemory; must be > 0
        // on a real JVM (classloader, test runner, JIT scaffolding are all alive).
        long used = Measurement.captureUsedMemoryAfterGc();
        assertThat(used).isGreaterThan(0L);
    }
}
