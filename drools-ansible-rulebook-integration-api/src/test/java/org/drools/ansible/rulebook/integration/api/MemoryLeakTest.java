package org.drools.ansible.rulebook.integration.api;

import org.junit.Ignore;
import org.junit.Test;

public class MemoryLeakTest {
    public static final String JSON1 =
            """
            {
                "rules": [
                        {
                            "Rule": {
                                "condition": {
                                    "AllCondition": [
                                        {
                                            "EqualsExpression": {
                                                "lhs": {
                                                    "Event": "i"
                                                },
                                                "rhs": {
                                                    "Integer": 1
                                                }
                                            }
                                        }
                                    ]
                                },
                                "enabled": true,
                                "name": null
                            }
                        }
                    ],
                "default_events_ttl" : "50000 seconds"
            }
            """;

    @Ignore("This test is to check for memory leaks. It should be run manually and not as part of the build.")
    @Test
    public void testMemoryLeakWithUnmatchEvents() {
        // If you set a short time for default_events_ttl, you can observe expiring jobs

        System.setProperty("org.slf4j.simpleLogger.log.org.drools.ansible.rulebook.integration", "INFO");
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON1);

        for (int i = 0; i < 2000000; i++) {
            rulesExecutor.processEvents( "{ \"i\": 5 }").join(); // not match

            if (i % 1000 == 0) {
                System.out.println("Processed " + i + " events");
                System.out.println("  " + rulesExecutor.getSessionStats());
                try {
                    Thread.sleep(100); // easier to capture a heap dump
                } catch (InterruptedException e) {
                    // ignore
                }
            }
        }
        rulesExecutor.dispose();
    }
}
