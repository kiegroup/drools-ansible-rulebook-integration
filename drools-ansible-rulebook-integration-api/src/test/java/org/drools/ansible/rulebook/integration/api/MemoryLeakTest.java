package org.drools.ansible.rulebook.integration.api;

import java.util.List;
import java.util.concurrent.TimeUnit;

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
            for (int i = 0; i < 100000; i++) {
                List<Match> matches = rulesExecutor.processEvents("{\"i\":5}").join();// not match
                assertThat(matches).isEmpty();

                if (i % 2000 == 0) {
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
            for (int i = 0; i < 100000; i++) {
                List<Match> matches = rulesExecutor.processEvents("{\"i\":1}").join();
                assertThat(matches).hasSize(1);

                if (i % 2000 == 0) {
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

            for (int i = 0; i < 100000; i++) {
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

                if (i % 2000 == 0) {
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
            assertThat(stats.getRulesTriggered()).isEqualTo(33330);
            assertThat(stats.getEventsMatched()).isEqualTo(33330);
            assertThat(stats.getEventsProcessed()).isEqualTo(100000);
            assertThat(stats.getEventsSuppressed()).isEqualTo(66670);
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

            for (int i = 0; i < 100000; i++) {
                String host = hosts[i % hosts.length];
                String event = String.format("{ \"sensu\": { \"process\": { \"type\":\"alert\" }, \"host\":\"%s\" } }", host);

                List<Match> matches = rulesExecutor.processEvents(event).join();

                // Should never fire since we only send 2 events per host before time expires
                assertThat(matches).isEmpty();

                // 1 second per event. For the same host, we will never reach the threshold of 10
                rulesExecutor.advanceTime(1, java.util.concurrent.TimeUnit.SECONDS);

                if (i % 2000 == 0) {
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
            assertThat(stats.getEventsProcessed()).isEqualTo(100000);
            assertThat(stats.getEventsSuppressed()).isEqualTo(100000);
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

    public static final String JSON_ONCE_WITHIN =
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
                                        "once_within": "10 seconds"
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
    public void testMemoryLeakWithOnceWithin() {
        System.setProperty("org.slf4j.simpleLogger.log.org.drools.ansible.rulebook.integration", "INFO");
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(RuleNotation.CoreNotation.INSTANCE.withOptions(RuleConfigurationOption.USE_PSEUDO_CLOCK), JSON_ONCE_WITHIN);
        System.gc();
        long baseMemory = rulesExecutor.getSessionStats().getUsedMemory();
        try {
            // Test with multiple hosts to create different control events
            String[] hosts = {"host1", "host2", "host3", "host4", "host5"};

            // Track event count per host
            int[] hostEventCounts = new int[hosts.length];

            for (int i = 0; i < 100000; i++) {
                int hostIndex = i % hosts.length;
                String host = hosts[hostIndex];
                String event = String.format("{ \"sensu\": { \"process\": { \"type\":\"alert\" }, \"host\":\"%s\" } }", host);

                List<Match> matches = rulesExecutor.processEvents(event).join();

                hostEventCounts[hostIndex]++;

                // within 10 seconds, the 1st event for each host triggers. the 2nd event is suppressed.
                if (hostEventCounts[hostIndex] % 2 == 1) {
                    assertThat(matches).hasSize(1);
                } else {
                    assertThat(matches).isEmpty();
                }

                rulesExecutor.advanceTime(1, java.util.concurrent.TimeUnit.SECONDS);

                if (i % 2000 == 0) {
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

            // Verify rules fired as expected
            SessionStats stats = rulesExecutor.getSessionStats();
            assertThat(stats.getRulesTriggered()).isEqualTo(50000);
            assertThat(stats.getEventsMatched()).isEqualTo(50000);
            assertThat(stats.getEventsProcessed()).isEqualTo(100000);
            assertThat(stats.getEventsSuppressed()).isEqualTo(50000);
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

    public static final String JSON_ONCE_AFTER =
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
                                        "once_after": "20 seconds"
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
    public void testMemoryLeakWithOnceAfter() {
        System.setProperty("org.slf4j.simpleLogger.log.org.drools.ansible.rulebook.integration", "WARN");
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(RuleNotation.CoreNotation.INSTANCE.withOptions(RuleConfigurationOption.USE_PSEUDO_CLOCK), JSON_ONCE_AFTER);
        System.gc();
        long baseMemory = rulesExecutor.getSessionStats().getUsedMemory();
        try {
            // Test with multiple hosts to create different control events
            String[] hosts = {"host1", "host2", "host3", "host4", "host5"};

            for (int i = 0; i < 100000; i++) {

                // 10 events for 5 different hosts. For 10 seconds.
                for (int j = 0; j < 10; j++) {
                    String host = hosts[j % hosts.length];
                    String event = String.format("{ \"sensu\": { \"process\": { \"type\":\"alert\" }, \"host\":\"%s\" } }", host);

                    List<Match> matches = rulesExecutor.processEvents(event).join();

                    // OnceAfter never fires immediately, only after window expires
                    assertThat(matches).isEmpty();

                    rulesExecutor.advanceTime(1, java.util.concurrent.TimeUnit.SECONDS);
                }

                // Expire the time window
                List<Match> matches = rulesExecutor.advanceTime(20, TimeUnit.SECONDS).join();
                assertThat(matches).hasSize(1);
                assertThat(matches.get(0).getObjects()).hasSize(5);

                if (i % 2000 == 0) {
                    System.gc();
                    System.out.println("  UsedMemory = " + rulesExecutor.getSessionStats().getUsedMemory());
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        // ignore
                    }
                }
            }

            // Final time advance to ensure all control events are processed
            rulesExecutor.advanceTime(30, java.util.concurrent.TimeUnit.SECONDS);

            SessionStats stats = rulesExecutor.getSessionStats();
            assertThat(stats.getRulesTriggered()).isEqualTo(100000);
            assertThat(stats.getEventsMatched()).isEqualTo(100000);
            assertThat(stats.getEventsProcessed()).isEqualTo(1000000);
            assertThat(stats.getEventsSuppressed()).isEqualTo(900000);
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

    public static final String JSON_TIMED_OUT =
            """
                    {
                        "rules": [
                            {
                                "Rule": {
                                    "condition": {
                                        "NotAllCondition": [
                                            {
                                                "EqualsExpression": {
                                                    "lhs": {
                                                        "Event": "sensu.process.status"
                                                    },
                                                    "rhs": {
                                                        "String": "stopped"
                                                    }
                                                }
                                            },
                                            {
                                                "EqualsExpression": {
                                                    "lhs": {
                                                        "Event": "ping.timeout"
                                                    },
                                                    "rhs": {
                                                        "Boolean": true
                                                    }
                                                }
                                            }
                                        ],
                                        "timeout": "5 seconds"
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
    public void testMemoryLeakWithTimedOut() {
        System.setProperty("org.slf4j.simpleLogger.log.org.drools.ansible.rulebook.integration", "WARN");
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(RuleNotation.CoreNotation.INSTANCE.withOptions(RuleConfigurationOption.USE_PSEUDO_CLOCK), JSON_TIMED_OUT);
        System.gc();
        long baseMemory = rulesExecutor.getSessionStats().getUsedMemory();
        try {
            for (int i = 0; i < 100000; i++) {

                // 1st scenario: send process status event
                String event = "{ \"sensu\": { \"process\": { \"status\":\"stopped\" } } }";
                List<Match> matches = rulesExecutor.processEvents(event).join();
                assertThat(matches).isEmpty(); // Should not fire, because not yet timed out

                matches = rulesExecutor.advanceTime(6, TimeUnit.SECONDS).join();
                assertThat(matches).hasSize(1); // Should fire because condition is satisfied after timeout

                // 2nd scenario: send ping timeout event
                event = "{ \"ping\": { \"timeout\": true } }";
                matches = rulesExecutor.processEvents(event).join();
                assertThat(matches).isEmpty(); // Should not fire, because not yet timed out

                matches = rulesExecutor.advanceTime(6, TimeUnit.SECONDS).join();
                assertThat(matches).hasSize(1); // Should fire because condition is satisfied after timeout

                // 3rd scenario: send both events
                event = "{ \"sensu\": { \"process\": { \"status\":\"stopped\" } } }";
                matches = rulesExecutor.processEvents(event).join();
                assertThat(matches).isEmpty();
                event = String.format("{ \"ping\": { \"timeout\": true } }");
                matches = rulesExecutor.processEvents(event).join();
                assertThat(matches).isEmpty();

                matches = rulesExecutor.advanceTime(6, TimeUnit.SECONDS).join();
                assertThat(matches).hasSize(0); // Should not fire because condition is not satisfied (NotAll)

                // Advance time by 3 seconds (less than 5 second timeout)

                if (i % 2000 == 0) {
                    System.gc();
                    System.out.println("  UsedMemory = " + rulesExecutor.getSessionStats().getUsedMemory());
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        // ignore
                    }
                }
            }

            // Final time advance to ensure all control events are processed
            rulesExecutor.advanceTime(10, java.util.concurrent.TimeUnit.SECONDS);

            SessionStats stats = rulesExecutor.getSessionStats();
            // Should have fired 50 times (every 20th iteration)
            assertThat(stats.getRulesTriggered()).isEqualTo(200000);
            assertThat(stats.getEventsMatched()).isEqualTo(200000);
            assertThat(stats.getEventsProcessed()).isEqualTo(400000);
            assertThat(stats.getEventsSuppressed()).isEqualTo(200000); // TimedOut doesn't suppress events
            assertThat(stats.getPermanentStorageCount()).isZero();

            // Allow some memory for the processing overhead
            long acceptableMemoryOverhead = 10 * 1000 * 1024; // 10 MB
            System.gc();
            long usedMemory = rulesExecutor.getSessionStats().getUsedMemory();
            assertThat(usedMemory).isLessThan(baseMemory + acceptableMemoryOverhead);
        } finally {
            rulesExecutor.dispose();
        }
    }

    public static final String JSON_TIME_WINDOW =
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
                                                        "Event": "ping.timeout"
                                                    },
                                                    "rhs": {
                                                        "Boolean": true
                                                    }
                                                }
                                            },
                                            {
                                                "EqualsExpression": {
                                                    "lhs": {
                                                        "Event": "sensu.process.status"
                                                    },
                                                    "rhs": {
                                                        "String": "stopped"
                                                    }
                                                }
                                            },
                                            {
                                                "GreaterThanExpression": {
                                                    "lhs": {
                                                        "Event": "sensu.storage.percent"
                                                    },
                                                    "rhs": {
                                                        "Integer": 95
                                                    }
                                                }
                                            }
                                        ],
                                        "timeout": "10 minutes"
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
    public void testMemoryLeakWithTimeWindow() {
        System.setProperty("org.slf4j.simpleLogger.log.org.drools.ansible.rulebook.integration", "INFO");
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(RuleNotation.CoreNotation.INSTANCE.withOptions(RuleConfigurationOption.USE_PSEUDO_CLOCK), JSON_TIME_WINDOW);
        System.gc();
        long baseMemory = rulesExecutor.getSessionStats().getUsedMemory();
        try {
            for (int i = 0; i < 100000; i++) {

                String event = "{ \"sensu\": { \"process\": { \"status\":\"stopped\" } } }";
                List<Match> matches = rulesExecutor.processEvents(event).join();
                assertThat(matches).isEmpty(); // Should not fire, need all 3 conditions

                rulesExecutor.advanceTime(8, TimeUnit.MINUTES);

                event = "{ \"ping\": { \"timeout\": true } }";
                matches = rulesExecutor.processEvents(event).join();
                assertThat(matches).isEmpty(); // Still need third condition

                // Advance time by 3 minutes (total 11 minutes, beyond 10 minutes window)
                rulesExecutor.advanceTime(3, TimeUnit.MINUTES);

                event = "{ \"sensu\": { \"storage\": { \"percent\":97 } } }";
                matches = rulesExecutor.processEvents(event).join();
                assertThat(matches).isEmpty(); //  the 1st event is out of the time window

                rulesExecutor.advanceTime(4, TimeUnit.MINUTES);

                event = "{ \"sensu\": { \"process\": { \"status\":\"stopped\" } } }";
                matches = rulesExecutor.processEvents(event).join();
                assertThat(matches).hasSize(1);

                if (i % 2000 == 0) {
                    System.gc();
                    System.out.println("  UsedMemory = " + rulesExecutor.getSessionStats().getUsedMemory());
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        // ignore
                    }
                }
            }

            // Final time advance to ensure all events are expired with default TTL 2 hours
            rulesExecutor.advanceTime(2, TimeUnit.HOURS);

            SessionStats stats = rulesExecutor.getSessionStats();
            assertThat(stats.getRulesTriggered()).isEqualTo(100000); // Should fire once per iteration
            assertThat(stats.getEventsMatched()).isEqualTo(300000); // 3 events matched per iteration
            assertThat(stats.getEventsProcessed()).isEqualTo(400000); // 4 events per iteration
            assertThat(stats.getEventsSuppressed()).isEqualTo(100000);
            assertThat(stats.getPermanentStorageCount()).isZero();

            // Allow some memory for the processing overhead
            long acceptableMemoryOverhead = 10 * 1000 * 1024; // 10 MB
            System.gc();
            long usedMemory = rulesExecutor.getSessionStats().getUsedMemory();
            assertThat(usedMemory).isLessThan(baseMemory + acceptableMemoryOverhead);
        } finally {
            rulesExecutor.dispose();
        }
    }
}
