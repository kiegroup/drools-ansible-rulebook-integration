package org.drools.ansible.rulebook.integration.ha.tests.integration.temporal;

import org.drools.ansible.rulebook.integration.ha.tests.support.TestUtils;

import org.drools.ansible.rulebook.integration.ha.tests.support.TestOutputCapture;

import org.drools.ansible.rulebook.integration.ha.tests.integration.HAIntegrationTestBase;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.readValueAsListOfMapOfStringAndObject;
import static org.drools.ansible.rulebook.integration.ha.tests.support.TestUtils.createEvent;

/**
 * Integration tests for TimeWindow with HA functionality.
 * TimeWindow requires multiple events to match within a time window.
 */
class HAIntegrationTimeWindowTest extends HAIntegrationTestBase {

    // TimeWindow rule - requires 3 different events within 10 seconds
    private static final String RULE_SET_TIME_WINDOW = """
                {
                    "name": "TimeWindow Ruleset",
                    "rules": [
                        {
                            "Rule": {
                                "name": "multi_event_correlation",
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
                                    "timeout": "10 seconds"
                                }
                            }
                        }
                    ]
                }
                """;

    @Override
    protected String getRuleSet() {
        return RULE_SET_TIME_WINDOW;
    }

    @Test
    void testSessionRecoveryWithTimeWindow() {
        // Step 1: Node 1 becomes leader and processes events
        rulesEngine1.enableLeader();

        // Process first event (t=0): sensu.process.status == "stopped"
        String firstEvent = createEvent("{\"sensu\":{\"process\":{\"status\":\"stopped\"}}}");
        String result1 = rulesEngine1.assertEvent(sessionId1, firstEvent);

        // Should NOT match - need all 3 events within window
        assertThat(readValueAsListOfMapOfStringAndObject(result1)).isEmpty();

        // Advance time by 3 seconds (t=3)
        rulesEngine1.advanceTime(sessionId1, 3, "SECONDS");

        // Process second event (t=3): ping.timeout == true
        String secondEvent = createEvent("{\"ping\":{\"timeout\":true}}");
        String result2 = rulesEngine1.assertEvent(sessionId1, secondEvent);

        // Should still NOT match - need all 3 events
        assertThat(readValueAsListOfMapOfStringAndObject(result2)).isEmpty();

        // Advance time by 2 more seconds (t=5)
        rulesEngine1.advanceTime(sessionId1, 2, "SECONDS");

        // Node2 should have the same time as Node1
        // This is important to advance time correctly during recoverSession
        rulesEngine2.advanceTime(sessionId2, 5, "SECONDS");

        // Step 2: Simulate Node 1 crash/shutdown
        rulesEngine1.disableLeader();
        rulesEngine1.close();
        rulesEngine1 = null;
        consumer1.stop();
        consumer1 = null;

        // Step 3: Node 2 takes over and recovers session
        // recovery happens here, advances to t=5
        rulesEngine2.enableLeader();

        // Step 4: Process third event (t=5): sensu.storage.percent > 95
        // All 3 events are now within the 10-second window
        String thirdEvent = createEvent("{\"sensu\":{\"storage\":{\"percent\":97}}}");
        String result3 = rulesEngine2.assertEvent(sessionId2, thirdEvent);

        // Should MATCH - all 3 events are within the window
        List<Map<String, Object>> matches3 = readValueAsListOfMapOfStringAndObject(result3);
        assertThat(matches3).hasSize(1);
        assertThat(matches3.get(0))
                .containsEntry("name", "multi_event_correlation");

        // Verify all 3 events are bound
        assertThat(((Map)matches3.get(0).get("events"))).containsKeys("m_0", "m_1", "m_2");
    }

    @Test
    void testSessionRecoveryWithEventsOutsideTimeWindow() {
        // This test verifies that temporal constraints are correctly enforced across recovery
        // when events are spread too far apart in time

        // Step 1: Node 1 becomes leader and processes first event
        rulesEngine1.enableLeader();

        // Process first event (t=0): sensu.process.status == "stopped"
        String firstEvent = createEvent("{\"sensu\":{\"process\":{\"status\":\"stopped\"}}}");
        String result1 = rulesEngine1.assertEvent(sessionId1, firstEvent);

        // Should NOT match - need all 3 events within window
        assertThat(readValueAsListOfMapOfStringAndObject(result1)).isEmpty();

        // Advance time by 3 seconds (t=3)
        rulesEngine1.advanceTime(sessionId1, 3, "SECONDS");

        // Process second event (t=3): ping.timeout == true
        String secondEvent = createEvent("{\"ping\":{\"timeout\":true}}");
        String result2 = rulesEngine1.assertEvent(sessionId1, secondEvent);

        // Should still NOT match - need all 3 events
        assertThat(readValueAsListOfMapOfStringAndObject(result2)).isEmpty();

        // Advance time by 2 more seconds (t=5)
        rulesEngine1.advanceTime(sessionId1, 2, "SECONDS");

        // Node2 should have the same time as Node1
        // This is important to advance time correctly during recoverSession
        rulesEngine2.advanceTime(sessionId2, 5, "SECONDS");

        // Step 2: Simulate Node 1 crash/shutdown
        rulesEngine1.disableLeader();
        rulesEngine1.close();
        rulesEngine1 = null;
        consumer1.stop();
        consumer1 = null;

        // Step 3: Node 2 takes over and recovers session
        // recovery happens here, advances to t=5
        rulesEngine2.enableLeader();

        // Step 4: Advance time significantly (t=5 + 9 = t=14)
        // This puts us more than 10 seconds away from the first event (t=0)
        rulesEngine2.advanceTime(sessionId2, 9, "SECONDS");

        // Process third event (t=14): sensu.storage.percent > 95
        String thirdEvent = createEvent("{\"sensu\":{\"storage\":{\"percent\":97}}}");
        String result3 = rulesEngine2.assertEvent(sessionId2, thirdEvent);

        // Should NOT match - Event 1 (t=0) and Event 3 (t=14) are 14 seconds apart (> 10 second window)
        // Even though we have all 3 event types in working memory:
        // - Event 1 (sensu.process.status, t=0)
        // - Event 2 (ping.timeout, t=3)
        // - Event 3 (sensu.storage.percent, t=14)
        // The time window constraint requires all events to be within 10 seconds of each other
        assertThat(readValueAsListOfMapOfStringAndObject(result3)).isEmpty();
    }

    @Test
    void testSessionRecoveryWithTimeWindowExpiredDuringOutage() {
        // Verifies that when a time window expires during an outage, WARN is logged.
        // Node 1 processes 2 events, then crashes. Node 2 recovers after the 10s window has elapsed.

        // Step 1: Node 1 becomes leader and processes events
        rulesEngine1.enableLeader();

        // Process first event (t=0): ping.timeout == true
        String firstEvent = createEvent("{\"ping\":{\"timeout\":true}}");
        String result1 = rulesEngine1.assertEvent(sessionId1, firstEvent);
        assertThat(readValueAsListOfMapOfStringAndObject(result1)).isEmpty();

        // Advance time by 2 seconds (t=2)
        rulesEngine1.advanceTime(sessionId1, 2, "SECONDS");

        // Process second event (t=2): sensu.process.status == "stopped"
        String secondEvent = createEvent("{\"sensu\":{\"process\":{\"status\":\"stopped\"}}}");
        String result2 = rulesEngine1.assertEvent(sessionId1, secondEvent);
        assertThat(readValueAsListOfMapOfStringAndObject(result2)).isEmpty();

        // Advance time by 1 second (t=3)
        rulesEngine1.advanceTime(sessionId1, 1, "SECONDS");

        // Node2 must match Node1's clock (t=3)
        rulesEngine2.advanceTime(sessionId2, 3, "SECONDS");

        // Step 2: Simulate Node 1 crash
        rulesEngine1.disableLeader();
        rulesEngine1.close();
        rulesEngine1 = null;
        consumer1.stop();
        consumer1 = null;

        // Step 3: Advance Node 2 clock past the window (t=3 + 9 = t=12, window expired at t=10)
        rulesEngine2.advanceTime(sessionId2, 9, "SECONDS");

        // Step 4: Node 2 takes over — recovery should detect expired sentinels and log WARN
        String logs;
        try {
            logs = TestOutputCapture.captureStdout(() -> rulesEngine2.enableLeader());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        assertThat(logs).contains("all+timeout window expired during outage for rule 'multi_event_correlation'");

        // Step 5: Even though we send the third event, the window has expired
        String thirdEvent = createEvent("{\"sensu\":{\"storage\":{\"percent\":97}}}");
        String result3 = rulesEngine2.assertEvent(sessionId2, thirdEvent);

        // Should NOT match — the first two events' sentinels expired during the outage
        assertThat(readValueAsListOfMapOfStringAndObject(result3)).isEmpty();
    }
}
