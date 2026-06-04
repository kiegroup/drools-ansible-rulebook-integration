package org.drools.ansible.rulebook.integration.ha.tests.integration.temporal;

import org.drools.ansible.rulebook.integration.ha.tests.support.TestUtils;

import org.drools.ansible.rulebook.integration.ha.tests.integration.HAIntegrationTestBase;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.readValueAsListOfMapOfStringAndObject;
import static org.drools.ansible.rulebook.integration.ha.tests.support.TestUtils.createEvent;

class HAIntegrationTimedOutTest extends HAIntegrationTestBase {

    private static final String RULE_SET_TIMED_OUT = """
                {
                    "name": "TimedOut Ruleset",
                    "rules": [
                        {"Rule": {
                            "name": "maint failed",
                            "condition": {
                                "NotAllCondition": [
                                    {
                                        "EqualsExpression": {
                                            "lhs": {
                                                "Event": "alert.code"
                                            },
                                            "rhs": {
                                                "Integer": 1001
                                            }
                                        }
                                    },
                                    {
                                        "EqualsExpression": {
                                            "lhs": {
                                                "Event": "alert.code"
                                            },
                                            "rhs": {
                                                "Integer": 1002
                                            }
                                        }
                                    }
                                ],
                                "timeout": "10 seconds"
                            },
                            "action": {
                                "Action": {
                                    "action": "print_event",
                                    "action_args": {}
                                }
                            },
                            "enabled": true
                        }}
                    ]
                }
                """;

    @Override
    protected String getRuleSet() {
        return RULE_SET_TIMED_OUT;
    }

    /**
     * Partial match on Node1 (code=1001), failover, Node2 advances past timeout.
     * Verify rule fires with matching_uuid.
     */
    @Test
    void testTimedOutSurvivesRecoveryAndFiresAfterTimeout() {
        rulesEngine1.enableLeader();

        // Send partial match: code=1001 (only 1 of 2 conditions met)
        String event = createEvent("{\"alert\":{\"code\":1001,\"message\":\"Applying maintenance\"}}");
        assertThat(readValueAsListOfMapOfStringAndObject(rulesEngine1.assertEvent(sessionId1, event))).isEmpty();

        // Advance Node1 to T=5s
        rulesEngine1.advanceTime(sessionId1, 5, "SECONDS");
        rulesEngine2.advanceTime(sessionId2, 5, "SECONDS");

        // Simulate failover
        rulesEngine1.disableLeader();
        rulesEngine1.close();
        rulesEngine1 = null;
        consumer1.stop();
        consumer1 = null;

        rulesEngine2.enableLeader();

        // Advance time past the 10-second timeout to trigger the rule
        String recoveryAdvanceResult = rulesEngine2.advanceTime(sessionId2, 6, "SECONDS");
        List<Map<String, Object>> matches = readValueAsListOfMapOfStringAndObject(recoveryAdvanceResult);

        assertThat(matches).hasSize(1);
        Map<String, Object> match = matches.get(0);
        assertThat(match)
                .containsEntry("name", "maint failed")
                .containsKey("matching_uuid");
    }

    /**
     * Partial match on Node1 (code=1001), failover, Node2 sends code=1002 within timeout.
     * Verify rule does NOT fire (all conditions met = timeout cancelled).
     */
    @Test
    void testAllConditionsMetAfterRecovery() {
        rulesEngine1.enableLeader();

        // Send partial match: code=1001
        String event1 = createEvent("{\"alert\":{\"code\":1001,\"message\":\"Applying maintenance\"}}");
        assertThat(readValueAsListOfMapOfStringAndObject(rulesEngine1.assertEvent(sessionId1, event1))).isEmpty();

        // Advance Node1 to T=3s
        rulesEngine1.advanceTime(sessionId1, 3, "SECONDS");
        rulesEngine2.advanceTime(sessionId2, 3, "SECONDS");

        // Simulate failover
        rulesEngine1.disableLeader();
        rulesEngine1.close();
        rulesEngine1 = null;
        consumer1.stop();
        consumer1 = null;

        rulesEngine2.enableLeader();

        // Send the second condition (code=1002) on Node2 within the timeout
        rulesEngine2.advanceTime(sessionId2, 1, "SECONDS");
        String event2 = createEvent("{\"alert\":{\"code\":1002,\"message\":\"Maintenance completed\"}}");
        assertThat(readValueAsListOfMapOfStringAndObject(rulesEngine2.assertEvent(sessionId2, event2))).isEmpty();

        // Advance past the timeout — rule should NOT fire because all conditions were met
        String result = rulesEngine2.advanceTime(sessionId2, 7, "SECONDS");
        List<Map<String, Object>> matches = readValueAsListOfMapOfStringAndObject(result);

        assertThat(matches)
                .as("All conditions met — timed_out rule should not fire")
                .isEmpty();
    }

    /**
     * No events before failover, send event on Node2 after.
     * Verify normal behavior (rule fires after timeout with partial match).
     */
    @Test
    void testTimedOutFailoverWithNoEvents() {
        rulesEngine1.enableLeader();

        // No events sent on Node1
        rulesEngine1.advanceTime(sessionId1, 3, "SECONDS");
        rulesEngine2.advanceTime(sessionId2, 3, "SECONDS");

        // Simulate failover
        rulesEngine1.disableLeader();
        rulesEngine1.close();
        rulesEngine1 = null;
        consumer1.stop();
        consumer1 = null;

        rulesEngine2.enableLeader();

        // Send partial match on Node2
        rulesEngine2.advanceTime(sessionId2, 1, "SECONDS");
        String event = createEvent("{\"alert\":{\"code\":1001,\"message\":\"Applying maintenance\"}}");
        assertThat(readValueAsListOfMapOfStringAndObject(rulesEngine2.assertEvent(sessionId2, event))).isEmpty();

        // Advance past the timeout — rule should fire
        String result = rulesEngine2.advanceTime(sessionId2, 11, "SECONDS");
        List<Map<String, Object>> matches = readValueAsListOfMapOfStringAndObject(result);

        assertThat(matches).hasSize(1);
        Map<String, Object> match = matches.get(0);
        assertThat(match)
                .containsEntry("name", "maint failed")
                .containsKey("matching_uuid");
    }

    /**
     * Event on Node1 at T=8s, Node2 clock at T=12s (past 10s timeout).
     * Failover triggers expiration during recovery.
     */
    @Test
    void testTimedOutExpiredDuringFailover() {
        rulesEngine1.enableLeader();

        // Send partial match: code=1001 at T=0
        String event = createEvent("{\"alert\":{\"code\":1001,\"message\":\"Applying maintenance\"}}");
        assertThat(readValueAsListOfMapOfStringAndObject(rulesEngine1.assertEvent(sessionId1, event))).isEmpty();

        // Advance Node1 to T=8s (timer will expire at T=10s)
        rulesEngine1.advanceTime(sessionId1, 8, "SECONDS");

        // Advance Node2 past the timeout
        rulesEngine2.advanceTime(sessionId2, 12, "SECONDS");

        // Simulate failover
        rulesEngine1.disableLeader();
        rulesEngine1.close();
        rulesEngine1 = null;
        consumer1.stop();
        consumer1 = null;

        // Node2 becomes leader — recovery advances clock from T=8s to T=12s
        // The timed_out timer expires during this clock jump
        rulesEngine2.enableLeader();

        // Advance time further — timer already expired during recovery
        String result = rulesEngine2.advanceTime(sessionId2, 5, "SECONDS");
        List<Map<String, Object>> matches = readValueAsListOfMapOfStringAndObject(result);
        assertThat(matches)
                .as("Timer already expired during recovery — no match from subsequent advanceTime")
                .isEmpty();
    }
}
