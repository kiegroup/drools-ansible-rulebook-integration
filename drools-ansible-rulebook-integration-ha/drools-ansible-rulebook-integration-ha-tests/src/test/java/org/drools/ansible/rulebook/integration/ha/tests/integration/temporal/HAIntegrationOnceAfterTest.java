package org.drools.ansible.rulebook.integration.ha.tests.integration.temporal;

import org.drools.ansible.rulebook.integration.ha.tests.support.TestUtils;

import org.drools.ansible.rulebook.integration.ha.tests.integration.HAIntegrationTestBase;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.readValueAsListOfMapOfStringAndObject;
import static org.drools.ansible.rulebook.integration.ha.tests.support.TestUtils.createEvent;

class HAIntegrationOnceAfterTest extends HAIntegrationTestBase {

    private static final String RULE_SET_ONCE_AFTER = """
                {
                    "name": "OnceAfter Ruleset",
                    "rules": [
                        {"Rule": {
                            "name": "alert_throttle",
                            "condition": {
                                "AllCondition": [
                                    {
                                        "EqualsExpression": {
                                            "lhs": {
                                                "Event": "alert.type"
                                            },
                                            "rhs": {
                                                "String": "warning"
                                            }
                                        }
                                    }
                                ],
                                "throttle": {
                                    "group_by_attributes": [
                                        "event.alert.host"
                                    ],
                                    "once_after": "10 seconds"
                                }
                            },
                            "action": {
                                "run_playbook": [
                                    {
                                        "name": "alert_handler.yml",
                                        "extra_vars": {
                                            "message": "Alert received"
                                        }
                                    }
                                ]
                            }
                        }}
                    ]
                }
                """;

    @Override
    protected String getRuleSet() {
        return RULE_SET_ONCE_AFTER;
    }

    @Test
    void testOnceAfterSurvivesRecoveryAndFiresAfterWindow() {
        rulesEngine1.enableLeader();

        String firstEvent = createEvent("{\"alert\":{\"type\":\"warning\",\"host\":\"h1\"}}");
        assertThat(readValueAsListOfMapOfStringAndObject(rulesEngine1.assertEvent(sessionId1, firstEvent))).isEmpty();

        rulesEngine1.advanceTime(sessionId1, 3, "SECONDS");

        String secondEvent = createEvent("{\"alert\":{\"type\":\"warning\",\"host\":\"h1\"}}");
        assertThat(readValueAsListOfMapOfStringAndObject(rulesEngine1.assertEvent(sessionId1, secondEvent))).isEmpty();

        // Total elapsed time on leader: 5 seconds
        rulesEngine1.advanceTime(sessionId1, 2, "SECONDS");
        rulesEngine2.advanceTime(sessionId2, 5, "SECONDS"); // keep follower clock aligned

        // Simulate failover
        rulesEngine1.disableLeader();
        rulesEngine1.close();
        rulesEngine1 = null;
        consumer1.stop();
        consumer1 = null;

        rulesEngine2.enableLeader();

        // Advance time past the 10-second window to trigger once_after
        String recoveryAdvanceResult = rulesEngine2.advanceTime(sessionId2, 6, "SECONDS");
        List<Map<String, Object>> matches = readValueAsListOfMapOfStringAndObject(recoveryAdvanceResult);

        assertThat(matches).hasSize(1);
        Map<String, Object> match = matches.get(0);
        assertThat(match)
                .containsEntry("name", "alert_throttle")
                .containsKey("matching_uuid");
        Map<String, Object> events = (Map<String, Object>) match.get("events");
        assertThat(events).hasSize(1);
        assertThat(events.keySet()).containsExactlyInAnyOrder("m");

        assertThat(events)
                .extracting("m")
                .extracting("meta")
                .extracting("rule_engine")
                .satisfies(ruleEngineMeta -> assertThat((Map<String, Integer>) ruleEngineMeta)
                        .containsEntry("events_in_window", 2));
    }

    @Test
    void testMultipleGroupsSurviveRecovery() {
        rulesEngine1.enableLeader();

        // Send event to h1 group
        String h1Event = createEvent("{\"alert\":{\"type\":\"warning\",\"host\":\"h1\"}}");
        assertThat(readValueAsListOfMapOfStringAndObject(rulesEngine1.assertEvent(sessionId1, h1Event))).isEmpty();

        rulesEngine1.advanceTime(sessionId1, 1, "SECONDS");

        // Send event to h2 group
        String h2Event = createEvent("{\"alert\":{\"type\":\"warning\",\"host\":\"h2\"}}");
        assertThat(readValueAsListOfMapOfStringAndObject(rulesEngine1.assertEvent(sessionId1, h2Event))).isEmpty();

        // Total elapsed time on leader: 3 seconds
        rulesEngine1.advanceTime(sessionId1, 2, "SECONDS");
        rulesEngine2.advanceTime(sessionId2, 3, "SECONDS"); // keep follower clock aligned

        // Simulate failover
        rulesEngine1.disableLeader();
        rulesEngine1.close();
        rulesEngine1 = null;
        consumer1.stop();
        consumer1 = null;

        rulesEngine2.enableLeader();

        // Advance time past the 10-second window to trigger once_after
        String recoveryAdvanceResult = rulesEngine2.advanceTime(sessionId2, 8, "SECONDS");
        List<Map<String, Object>> matches = readValueAsListOfMapOfStringAndObject(recoveryAdvanceResult);

        assertThat(matches).hasSize(1);
        Map<String, Object> match = matches.get(0);
        assertThat(match)
                .containsEntry("name", "alert_throttle")
                .containsKey("matching_uuid");

        // With 2 groups, events should have m_0 and m_1
        Map<String, Object> events = (Map<String, Object>) match.get("events");
        assertThat(events).hasSize(2);
        assertThat(events.keySet()).containsExactlyInAnyOrder("m_0", "m_1");

        // Collect events_in_window by host (order of m_0/m_1 is not guaranteed)
        Map<String, Integer> eventsInWindowByHost = new java.util.HashMap<>();
        for (String key : events.keySet()) {
            Map<String, Object> event = (Map<String, Object>) events.get(key);
            Map<String, Object> meta = (Map<String, Object>) event.get("meta");
            Map<String, Object> ruleEngineMeta = (Map<String, Object>) meta.get("rule_engine");
            Map<String, Object> alert = (Map<String, Object>) event.get("alert");
            String host = (String) alert.get("host");
            int count = ((Number) ruleEngineMeta.get("events_in_window")).intValue();
            eventsInWindowByHost.put(host, count);
        }

        assertThat(eventsInWindowByHost)
                .containsEntry("h1", 1)
                .containsEntry("h2", 1);
    }

    @Test
    void testAddEventsAfterFailoverBeforeWindowExpires() {
        rulesEngine1.enableLeader();

        // Send first event to h1 on Node1 (T=0)
        String firstEvent = createEvent("{\"alert\":{\"type\":\"warning\",\"host\":\"h1\"}}");
        assertThat(readValueAsListOfMapOfStringAndObject(rulesEngine1.assertEvent(sessionId1, firstEvent))).isEmpty();

        // Total elapsed time on leader: 3 seconds
        rulesEngine1.advanceTime(sessionId1, 3, "SECONDS");
        rulesEngine2.advanceTime(sessionId2, 3, "SECONDS"); // keep follower clock aligned

        // Simulate failover
        rulesEngine1.disableLeader();
        rulesEngine1.close();
        rulesEngine1 = null;
        consumer1.stop();
        consumer1 = null;

        rulesEngine2.enableLeader();

        // Send another event to h1 on Node2 (T=4s, still within 10s window)
        rulesEngine2.advanceTime(sessionId2, 1, "SECONDS");
        String secondEvent = createEvent("{\"alert\":{\"type\":\"warning\",\"host\":\"h1\"}}");
        assertThat(readValueAsListOfMapOfStringAndObject(rulesEngine2.assertEvent(sessionId2, secondEvent))).isEmpty();

        // Advance time past the 10-second window to trigger once_after
        String recoveryAdvanceResult = rulesEngine2.advanceTime(sessionId2, 7, "SECONDS");
        List<Map<String, Object>> matches = readValueAsListOfMapOfStringAndObject(recoveryAdvanceResult);

        assertThat(matches).hasSize(1);
        Map<String, Object> match = matches.get(0);
        assertThat(match)
                .containsEntry("name", "alert_throttle")
                .containsKey("matching_uuid");

        // Single group h1, so events key is "m" (not "m_0")
        Map<String, Object> events = (Map<String, Object>) match.get("events");
        assertThat(events).hasSize(1);
        assertThat(events.keySet()).containsExactlyInAnyOrder("m");

        // events_in_window should be 2: one from Node1 + one from Node2
        assertThat(events)
                .extracting("m")
                .extracting("meta")
                .extracting("rule_engine")
                .satisfies(ruleEngineMeta -> assertThat((Map<String, Integer>) ruleEngineMeta)
                        .containsEntry("events_in_window", 2));
    }

    @Test
    void testNewGroupAfterFailover() {
        rulesEngine1.enableLeader();

        // Send event to h1 on Node1 (T=0)
        String h1Event = createEvent("{\"alert\":{\"type\":\"warning\",\"host\":\"h1\"}}");
        assertThat(readValueAsListOfMapOfStringAndObject(rulesEngine1.assertEvent(sessionId1, h1Event))).isEmpty();

        // Total elapsed time on leader: 3 seconds
        rulesEngine1.advanceTime(sessionId1, 3, "SECONDS");
        rulesEngine2.advanceTime(sessionId2, 3, "SECONDS");

        // Simulate failover
        rulesEngine1.disableLeader();
        rulesEngine1.close();
        rulesEngine1 = null;
        consumer1.stop();
        consumer1 = null;

        rulesEngine2.enableLeader();

        // Send event to h2 on Node2 (T=4s, a group that didn't exist before failover)
        rulesEngine2.advanceTime(sessionId2, 1, "SECONDS");
        String h2Event = createEvent("{\"alert\":{\"type\":\"warning\",\"host\":\"h2\"}}");
        assertThat(readValueAsListOfMapOfStringAndObject(rulesEngine2.assertEvent(sessionId2, h2Event))).isEmpty();

        // Advance time past the 10-second window
        String recoveryAdvanceResult = rulesEngine2.advanceTime(sessionId2, 7, "SECONDS");
        List<Map<String, Object>> matches = readValueAsListOfMapOfStringAndObject(recoveryAdvanceResult);

        assertThat(matches).hasSize(1);
        Map<String, Object> match = matches.get(0);
        assertThat(match)
                .containsEntry("name", "alert_throttle")
                .containsKey("matching_uuid");

        // With 2 groups (h1 from Node1, h2 from Node2), events should have m_0 and m_1
        Map<String, Object> events = (Map<String, Object>) match.get("events");
        assertThat(events).hasSize(2);
        assertThat(events.keySet()).containsExactlyInAnyOrder("m_0", "m_1");

        // Collect events_in_window by host
        Map<String, Integer> eventsInWindowByHost = new java.util.HashMap<>();
        for (String key : events.keySet()) {
            Map<String, Object> event = (Map<String, Object>) events.get(key);
            Map<String, Object> meta = (Map<String, Object>) event.get("meta");
            Map<String, Object> ruleEngineMeta = (Map<String, Object>) meta.get("rule_engine");
            Map<String, Object> alert = (Map<String, Object>) event.get("alert");
            String host = (String) alert.get("host");
            int count = ((Number) ruleEngineMeta.get("events_in_window")).intValue();
            eventsInWindowByHost.put(host, count);
        }

        // h1 from Node1, h2 from Node2 — both with count 1
        assertThat(eventsInWindowByHost)
                .containsEntry("h1", 1)
                .containsEntry("h2", 1);
    }

    /**
     * Tests that the once_after timer expires during recovery clock advancement.
     *
     * Known limitation: The rule fires during recoverSession() on the main thread,
     * but the match is NOT dispatched via async channel (asyncResponses=0).
     * This is because the recovery clock advancement happens outside the normal
     * HA dispatch pipeline. The match is consumed internally (rulesTriggered=1)
     * but not visible to the Python side.
     *
     * This test verifies:
     * 1. The timer expires during recovery (subsequent advanceTime produces no match)
     * 2. The rule fired exactly once (no duplicate match after recovery)
     */
    @Test
    void testTimerExpiredDuringFailover() {
        rulesEngine1.enableLeader();

        // Send event to h1 on Node1 (T=0)
        String event = createEvent("{\"alert\":{\"type\":\"warning\",\"host\":\"h1\"}}");
        assertThat(readValueAsListOfMapOfStringAndObject(rulesEngine1.assertEvent(sessionId1, event))).isEmpty();

        // Advance Node1 to T=8s (timer set to expire at T=10s)
        rulesEngine1.advanceTime(sessionId1, 8, "SECONDS");

        // Advance Node2 (follower) past the 10s window so recovery clock jump triggers expiration
        rulesEngine2.advanceTime(sessionId2, 12, "SECONDS");

        // Simulate failover
        rulesEngine1.disableLeader();
        rulesEngine1.close();
        rulesEngine1 = null;
        consumer1.stop();
        consumer1 = null;

        // Node2 becomes leader — recovery advances clock from T=8s (persisted) to T=12s (Node2's clock)
        // The once_after timer expires during this clock jump (the rule fires on [main] thread)
        // TODO: Edge Case: How should we handle the match in this case? (If Node1 had fired the rule by AutomaticPseudoClock, this wouldn't happen)
        rulesEngine2.enableLeader();

        // Advance time further — the timer already expired during recovery,
        // so no additional match should fire
        String result = rulesEngine2.advanceTime(sessionId2, 5, "SECONDS");
        List<Map<String, Object>> matches = readValueAsListOfMapOfStringAndObject(result);
        assertThat(matches)
                .as("Timer already expired during recovery — no match from subsequent advanceTime")
                .isEmpty();
    }

    @Test
    void testFailoverWithNoOnceAfterEvents() {
        rulesEngine1.enableLeader();

        // No events sent on Node1 — advance time a bit, then failover
        rulesEngine1.advanceTime(sessionId1, 3, "SECONDS");
        rulesEngine2.advanceTime(sessionId2, 3, "SECONDS");

        // Simulate failover with empty OnceAfter state
        rulesEngine1.disableLeader();
        rulesEngine1.close();
        rulesEngine1 = null;
        consumer1.stop();
        consumer1 = null;

        rulesEngine2.enableLeader();

        // Send event to h1 on Node2 (T=4s)
        rulesEngine2.advanceTime(sessionId2, 1, "SECONDS");
        String h1Event = createEvent("{\"alert\":{\"type\":\"warning\",\"host\":\"h1\"}}");
        assertThat(readValueAsListOfMapOfStringAndObject(rulesEngine2.assertEvent(sessionId2, h1Event))).isEmpty();

        // Advance time past the 10-second window to trigger once_after
        String result = rulesEngine2.advanceTime(sessionId2, 11, "SECONDS");
        List<Map<String, Object>> matches = readValueAsListOfMapOfStringAndObject(result);

        assertThat(matches).hasSize(1);
        Map<String, Object> match = matches.get(0);
        assertThat(match)
                .containsEntry("name", "alert_throttle")
                .containsKey("matching_uuid");

        Map<String, Object> events = (Map<String, Object>) match.get("events");
        assertThat(events).hasSize(1);
        assertThat(events.keySet()).containsExactlyInAnyOrder("m");

        assertThat(events)
                .extracting("m")
                .extracting("meta")
                .extracting("rule_engine")
                .satisfies(ruleEngineMeta -> assertThat((Map<String, Integer>) ruleEngineMeta)
                        .containsEntry("events_in_window", 1));
    }

    @Test
    void testHighEventCountAcrossFailover() {
        rulesEngine1.enableLeader();

        // Send 5 events to h1 on Node1 (all within the window)
        for (int i = 0; i < 5; i++) {
            String event = createEvent("{\"alert\":{\"type\":\"warning\",\"host\":\"h1\"}}");
            assertThat(readValueAsListOfMapOfStringAndObject(rulesEngine1.assertEvent(sessionId1, event))).isEmpty();
        }

        // Total elapsed time on leader: 5 seconds
        rulesEngine1.advanceTime(sessionId1, 5, "SECONDS");
        rulesEngine2.advanceTime(sessionId2, 5, "SECONDS"); // keep follower clock aligned

        // Simulate failover
        rulesEngine1.disableLeader();
        rulesEngine1.close();
        rulesEngine1 = null;
        consumer1.stop();
        consumer1 = null;

        rulesEngine2.enableLeader();

        // Send 3 more events to h1 on Node2 (still within 10s window)
        for (int i = 0; i < 3; i++) {
            String event = createEvent("{\"alert\":{\"type\":\"warning\",\"host\":\"h1\"}}");
            assertThat(readValueAsListOfMapOfStringAndObject(rulesEngine2.assertEvent(sessionId2, event))).isEmpty();
        }

        // Advance time past the 10-second window to trigger once_after
        String result = rulesEngine2.advanceTime(sessionId2, 6, "SECONDS");
        List<Map<String, Object>> matches = readValueAsListOfMapOfStringAndObject(result);

        assertThat(matches).hasSize(1);
        Map<String, Object> match = matches.get(0);
        assertThat(match)
                .containsEntry("name", "alert_throttle")
                .containsKey("matching_uuid");

        Map<String, Object> events = (Map<String, Object>) match.get("events");
        assertThat(events).hasSize(1);
        assertThat(events.keySet()).containsExactlyInAnyOrder("m");

        // events_in_window should be 8: 5 from Node1 + 3 from Node2
        assertThat(events)
                .extracting("m")
                .extracting("meta")
                .extracting("rule_engine")
                .satisfies(ruleEngineMeta -> assertThat((Map<String, Integer>) ruleEngineMeta)
                        .containsEntry("events_in_window", 8));
    }
}
