package org.drools.ansible.rulebook.integration.api;

import java.util.List;

import org.drools.ansible.rulebook.integration.api.rulesengine.SessionStats;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
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

    public static final String EVENT_24KB_UNMATCH = "{\"i\":5,\"data\":\"" + "A".repeat(24 * 1024) + "\"}";

    @Disabled("disabled by default as this could be unstable." +
            " Also this test may flood the logs with DEBUG messages (SimpleLogger cannot change the log level dynamically)")
    @Test
    @Timeout(120)
    void testMemoryLeakWithUnmatchEvents() {
        // If you set a short time for default_events_ttl, you can observe expiring jobs

        System.setProperty("org.slf4j.simpleLogger.log.org.drools.ansible.rulebook.integration", "INFO");
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON_TTL);
        System.gc();
        long baseMemory = rulesExecutor.getSessionStats().getUsedMemory();
        try {
            for (int i = 0; i < 10000; i++) {
                // The 24KB isn’t the condition to reproduce; it’s just to make checking the heap size easier.
                List<Match> matches = rulesExecutor.processEvents(EVENT_24KB_UNMATCH).join();// not match
                assertThat(matches).isEmpty();

                if (i % 100 == 0) {
                    System.gc();
                    System.out.println("  UsedMemory = " + rulesExecutor.getSessionStats().getUsedMemory());
                    try {
                        Thread.sleep(100); // easier to capture a heap dump
                    } catch (InterruptedException e) {
                        // ignore
                    }
                }
            }
            // Allow some memory for the processing overhead
            // The acceptableMemoryOverhead may not be a critical threshold. If the test fails, you may consider increasing it unless it's not a memory leak.
            long acceptableMemoryOverhead = 10 * 1000 * 1024; // 10 MB
            System.gc();
            long usedMemory = rulesExecutor.getSessionStats().getUsedMemory();
            assertThat(usedMemory).isLessThan(baseMemory + acceptableMemoryOverhead);
        } finally {
            rulesExecutor.dispose();
        }
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

    @Disabled("disabled by default as this could be unstable." +
            " Also this test may flood the logs with DEBUG messages (SimpleLogger cannot change the log level dynamically)")
    @Test
    @Timeout(120)
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

                if (i % 100 == 0) {
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
            // The acceptableMemoryOverhead may not be a critical threshold. If the test fails, you may consider increasing it unless it's not a memory leak.
            long acceptableMemoryOverhead = 10 * 1000 * 1024; // 10 MB
            System.gc();
            long usedMemory = rulesExecutor.getSessionStats().getUsedMemory();
            assertThat(usedMemory).isLessThan(baseMemory + acceptableMemoryOverhead);
        } finally {
            rulesExecutor.dispose();
        }
    }

    public static final String JSON_ACCUMULATE_WITHIN =
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
                                                        "Event": "sensu.process.type"
                                                    },
                                                    "rhs": {
                                                        "String": "alert"
                                                    }
                                                }
                                            }
                                        ]
                                    },
                                    "throttle": {
                                        "group_by_attributes": [
                                            "event.sensu.host",
                                            "event.sensu.process.type"
                                        ],
                                        "accumulate_within": "20 seconds",
                                        "threshold": 3
                                    },
                                    "enabled": true,
                                    "name": null
                                }
                            }
                        ]
                    }
                    """;

    @Disabled("disabled by default as this could be unstable." +
            " Also this test may flood the logs with DEBUG messages (SimpleLogger cannot change the log level dynamically)")
    @Test
    @Timeout(120)
    public void testMemoryLeakWithAccumulateWithin() {
        System.setProperty("org.slf4j.simpleLogger.log.org.drools.ansible.rulebook.integration", "INFO");
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(RuleNotation.CoreNotation.INSTANCE.withOptions(RuleConfigurationOption.USE_PSEUDO_CLOCK), JSON_ACCUMULATE_WITHIN);
        System.gc();
        long baseMemory = rulesExecutor.getSessionStats().getUsedMemory();
        try {
            // Test with multiple hosts to create different control events
            String[] hosts = {"host1", "host2", "host3", "host4", "host5"};

            // Track event count per host
            int[] hostEventCounts = new int[hosts.length];

            for (int i = 0; i < 10000; i++) {
                int hostIndex = i % hosts.length;
                String host = hosts[hostIndex];
                String event = String.format("{ \"sensu\": { \"process\": { \"type\":\"alert\" }, \"host\":\"%s\" } }", host);

                List<Match> matches = rulesExecutor.processEvents(event).join();

                hostEventCounts[hostIndex]++;

                // Every 3rd event for the same host should trigger
                if (hostEventCounts[hostIndex] % 3 == 0) {
                    assertThat(matches).hasSize(1);
                } else {
                    assertThat(matches).isEmpty();
                }

                rulesExecutor.advanceTime(1, java.util.concurrent.TimeUnit.SECONDS);

                if (i % 100 == 0) {
                    System.gc();
                    System.out.println("  UsedMemory = " + rulesExecutor.getSessionStats().getUsedMemory());
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        // ignore
                    }
                }
            }

            // Final time advance to ensure all control events expire
            rulesExecutor.advanceTime(25, java.util.concurrent.TimeUnit.SECONDS);

            SessionStats stats = rulesExecutor.getSessionStats();
            assertThat(stats.getRulesTriggered()).isEqualTo(3330);
            assertThat(stats.getEventsMatched()).isEqualTo(3330);
            assertThat(stats.getEventsProcessed()).isEqualTo(10000);
            assertThat(stats.getEventsSuppressed()).isEqualTo(6670);
            assertThat(stats.getPermanentStorageCount()).isZero();

            // Allow some memory for the processing overhead
            long acceptableMemoryOverhead = 10 * 1000 * 1024;
            System.gc();
            long usedMemory = stats.getUsedMemory();
            assertThat(usedMemory).isLessThan(baseMemory + acceptableMemoryOverhead);
        } finally {
            rulesExecutor.dispose();
        }
    }

    public static final String JSON_ACCUMULATE_WITHIN_NO_THRESHOLD =
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
                                                        "Event": "sensu.process.type"
                                                    },
                                                    "rhs": {
                                                        "String": "alert"
                                                    }
                                                }
                                            }
                                        ]
                                    },
                                    "throttle": {
                                        "group_by_attributes": [
                                            "event.sensu.host"
                                        ],
                                        "accumulate_within": "10 seconds",
                                        "threshold": 10
                                    },
                                    "enabled": true,
                                    "name": null
                                }
                            }
                        ]
                    }
                    """;

    @Disabled("disabled by default as this could be unstable." +
            " Also this test may flood the logs with DEBUG messages (SimpleLogger cannot change the log level dynamically)")
    @Test
    @Timeout(120)
    public void testMemoryLeakWithAccumulateWithinBelowThreshold() {
        System.setProperty("org.slf4j.simpleLogger.log.org.drools.ansible.rulebook.integration", "INFO");
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(RuleNotation.CoreNotation.INSTANCE.withOptions(RuleConfigurationOption.USE_PSEUDO_CLOCK), JSON_ACCUMULATE_WITHIN_NO_THRESHOLD);
        System.gc();
        long baseMemory = rulesExecutor.getSessionStats().getUsedMemory();
        try {
            // Test with multiple hosts, but never reach threshold
            String[] hosts = {"host1", "host2", "host3", "host4", "host5"};

            for (int i = 0; i < 10000; i++) {
                String host = hosts[i % hosts.length];
                String event = String.format("{ \"sensu\": { \"process\": { \"type\":\"alert\" }, \"host\":\"%s\" } }", host);

                List<Match> matches = rulesExecutor.processEvents(event).join();

                // Should never fire since we only send 2 events per host before time expires
                assertThat(matches).isEmpty();

                // 1 second per event. For the same host, we will never reach the threshold of 10
                rulesExecutor.advanceTime(1, java.util.concurrent.TimeUnit.SECONDS);

                if (i % 100 == 0) {
                    System.gc();
                    System.out.println("  UsedMemory = " + rulesExecutor.getSessionStats().getUsedMemory());
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        // ignore
                    }
                }
            }

            // Final time advance to ensure all control events expire
            rulesExecutor.advanceTime(15, java.util.concurrent.TimeUnit.SECONDS);

            SessionStats stats = rulesExecutor.getSessionStats();
            assertThat(stats.getRulesTriggered()).isZero();
            assertThat(stats.getEventsMatched()).isZero();
            assertThat(stats.getEventsProcessed()).isEqualTo(10000);
            assertThat(stats.getEventsSuppressed()).isEqualTo(10000);
            assertThat(stats.getPermanentStorageCount()).isZero();

            // Allow some memory for the processing overhead
            long acceptableMemoryOverhead = 10 * 1000 * 1024; // 10 MB
            System.gc();
            long usedMemory = stats.getUsedMemory();
            assertThat(usedMemory).isLessThan(baseMemory + acceptableMemoryOverhead);
        } finally {
            rulesExecutor.dispose();
        }
    }
}
