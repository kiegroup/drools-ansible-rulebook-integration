package org.drools.ansible.rulebook.integration.api;

import java.util.List;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.kie.api.runtime.rule.Match;

import static org.assertj.core.api.Assertions.assertThat;


public class MemoryLeakTest {
    public static final String JSON_TTL =
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

    @Test
    void testMemoryLeakWithUnmatchEvents() {
        // If you set a short time for default_events_ttl, you can observe expiring jobs

        System.setProperty("org.slf4j.simpleLogger.log.org.drools.ansible.rulebook.integration", "INFO");
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON_TTL);

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

    public static final String JSON_PLAIN =
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
                    ]
            }
            """;

    public static final String EVENT_24KB = "{\"i\":1,\"data\":\"" + "A".repeat(24 * 1024) + "\"}";

    @Test
    public void testMemoryLeakWithMatchingEvents() {
        System.setProperty("org.slf4j.simpleLogger.log.org.drools.ansible.rulebook.integration", "INFO");
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON_PLAIN);
        System.gc();
        long baseMemory = rulesExecutor.getSessionStats().getUsedMemory();
        try {
            for (int i = 0; i < 10000; i++) {
                // The 24KB isn’t the condition to reproduce; it’s just to make checking the heap size easier.
                List<Match> matches = rulesExecutor.processEvents(EVENT_24KB).join();
                assertThat(matches).hasSize(1);

                if (i % 20 == 0) {
                    System.out.println("Processed " + i + " events. Calling GC");
                    System.gc();
                    System.out.println("  usedMemory = " + rulesExecutor.getSessionStats().getUsedMemory());
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        // ignore
                    }
                }
            }

            // Allow some memory for the processing overhead
            // The memoryOverhead may not be a critical threshold. If the test fails, you may consider increasing it unless it's not a memory leak.
            long memoryOverhead = 10 * 1000 * 1024; // 10 MB
            System.gc();
            long usedMemory = rulesExecutor.getSessionStats().getUsedMemory();
            assertThat(usedMemory).isLessThan(baseMemory + memoryOverhead);
        } finally {
            rulesExecutor.dispose();
        }
    }
}
