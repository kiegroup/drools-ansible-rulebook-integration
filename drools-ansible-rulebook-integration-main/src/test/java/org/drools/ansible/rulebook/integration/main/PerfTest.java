package org.drools.ansible.rulebook.integration.main;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class PerfTest {

    @Test
    public void test() {
        long duration = Main.execute("100k_event_rules_ast.json");
        System.out.println("Executed in " + duration + " msecs");
        assertTrue("There is a performance issue, this test took too long: " + duration + " msecs", duration < 10_000);
    }
}
