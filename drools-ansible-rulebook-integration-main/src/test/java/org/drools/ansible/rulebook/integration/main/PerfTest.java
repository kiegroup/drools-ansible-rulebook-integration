package org.drools.ansible.rulebook.integration.main;

import org.drools.ansible.rulebook.integration.api.rulesengine.MemoryThresholdReachedException;
import org.drools.ansible.rulebook.integration.main.Main.ExecuteResult;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.drools.ansible.rulebook.integration.api.rulesengine.RuleEngineTestUtils.disableEventStructureSuggestion;
import static org.drools.ansible.rulebook.integration.api.rulesengine.RuleEngineTestUtils.enableEventStructureSuggestion;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PerfTest {

    @Test
    void testManyEvents() {
        checkDuration("100k_event_rules_ast.json", 10_000);
    }

    @Disabled("Disabled by default, because it takes around 40 seconds")
    @Test
    void testManyLargeEvents() {
        // match_multiple_rules: false means events are removed after match. So this test will pass without throwing MemoryThresholdReachedException
        checkDuration("1m_event_with_20kb_payload_rules_ast.json", 120_000);
    }

    @Test
    void testManyLargeEventsMatchMultipleRules() {
        // match_multiple_rules: true means events are retained until TTL expires
        assertThrows(MemoryThresholdReachedException.class, () -> checkDuration("1m_event_with_20kb_payload_match_multiple_rules_ast.json", 120_000));
    }

    @Test
    void testOnceAfter() {
        checkDuration("56_once_after.json", 15_000);
    }

    private static void checkDuration(String jsonFile, int expectedMaxDuration) {
        ExecuteResult result = Main.execute(jsonFile);
        long duration = result.getDuration();
        System.out.println("Executed in " + duration + " msecs");
        assertTrue(duration < expectedMaxDuration, "There is a performance issue, this test took too long: " + duration + " msecs");
    }

    @Test
    void test1000RulesWithEventStructureSuggestion() {
        try {
            enableEventStructureSuggestion();
            checkDuration("1k_rules_ast.json", 120_000);
        } finally {
            disableEventStructureSuggestion();
        }
    }
}
