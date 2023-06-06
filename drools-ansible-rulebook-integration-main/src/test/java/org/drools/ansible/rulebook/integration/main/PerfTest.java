package org.drools.ansible.rulebook.integration.main;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class PerfTest {

    @Test
    public void testManyEvents() {
        checkDuration("100k_event_rules_ast.json", 10_000);
    }

    @Test
    public void testOnceAfter() {
        checkDuration("56_once_after.json", 15_000);
    }

    private static void checkDuration(String jsonFile, int expectedMaxDuration) {
        long duration = Main.execute(jsonFile);
        System.out.println("Executed in " + duration + " msecs");
        assertTrue("There is a performance issue, this test took too long: " + duration + " msecs", duration < expectedMaxDuration);
    }
}
