package org.drools.ansible.rulebook.integration.ha.tests;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.readValueAsListOfMapOfStringAndObject;
import static org.drools.ansible.rulebook.integration.ha.tests.TestUtils.createEvent;

/**
 * Integration tests for AccumulateWithin with HA functionality.
 * AccumulateWithin accumulates events until a threshold is met within a time window.
 */
class HAIntegrationAccumulateWithinTest extends HAIntegrationTestBase {

    // AccumulateWithin rule - fires when 3 events arrive within 10 seconds for the same host
    private static final String RULE_SET_ACCUMULATE_WITHIN = """
                {
                    "name": "AccumulateWithin Ruleset",
                    "rules": [
                        {
                            "Rule": {
                                "name": "alert_accumulator",
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
                                    "accumulate_within": "10 seconds",
                                    "threshold": 3
                                },
                                "action": {
                                    "run_playbook": [
                                        {
                                            "name": "alert_handler.yml"
                                        }
                                    ]
                                }
                            }
                        }
                    ]
                }
                """;

    @Override
    protected String getRuleSet() {
        return RULE_SET_ACCUMULATE_WITHIN;
    }

    @Test
    void testSessionRecoveryWithAccumulateWithin() {
        // This test verifies that the accumulation counter is correctly restored across recovery

        // Step 1: Node 1 becomes leader and processes events
        rulesEngine1.enableLeader();

        // Process first event (t=0, count=0->1)
        String firstEvent = createEvent("{\"sensu\":{\"process\":{\"type\":\"alert\"},\"host\":\"h1\"},\"sequence\":1}");
        String result1 = rulesEngine1.assertEvent(sessionId1, firstEvent);

        // Should NOT match - count is 1, need 3
        assertThat(readValueAsListOfMapOfStringAndObject(result1)).isEmpty();

        // Advance time by 2 seconds (t=2)
        rulesEngine1.advanceTime(sessionId1, 2, "SECONDS");

        // Process second event (t=2, count=1->2)
        String secondEvent = createEvent("{\"sensu\":{\"process\":{\"type\":\"alert\"},\"host\":\"h1\"},\"sequence\":2}");
        String result2 = rulesEngine1.assertEvent(sessionId1, secondEvent);

        // Should still NOT match - count is 2, need 3
        assertThat(readValueAsListOfMapOfStringAndObject(result2)).isEmpty();

        // Advance time by 1 more second (t=3)
        rulesEngine1.advanceTime(sessionId1, 1, "SECONDS");

        // Node2 should have the same time as Node1
        // This is important to advance time correctly during recoverSession
        rulesEngine2.advanceTime(sessionId2, 3, "SECONDS");

        // Step 2: Simulate Node 1 crash/shutdown
        rulesEngine1.disableLeader();
        rulesEngine1.close();
        rulesEngine1 = null;
        consumer1.stop();
        consumer1 = null;

        // Step 3: Node 2 takes over and recovers session
        // recovery happens here, restores control event with current_count=2, advances to t=3
        rulesEngine2.enableLeader();

        // Step 4: Process third event (t=3, count=2->3)
        // This should trigger the rule because threshold (3) is reached
        String thirdEvent = createEvent("{\"sensu\":{\"process\":{\"type\":\"alert\"},\"host\":\"h1\"},\"sequence\":3}");
        String result3 = rulesEngine2.assertEvent(sessionId2, thirdEvent);

        // Should MATCH - count reached threshold of 3
        List<Map<String, Object>> matches3 = readValueAsListOfMapOfStringAndObject(result3);
        assertThat(matches3).hasSize(1);
        assertThat(matches3.get(0))
                .containsEntry("name", "alert_accumulator")
                .containsKey("matching_uuid");

        // Verify the returned event is the triggering event (sequence=3)
        Map<String, Object> eventData = (Map<String, Object>) matches3.get(0).get("events");
        Map<String, Object> matchedEvent = (Map<String, Object>) eventData.get("m");
        assertThat(matchedEvent).containsEntry("sequence", 3);
    }

    @Test
    void testSessionRecoveryWithAccumulateWindowExpiredDuringOutage() {
        // This test verifies that when the accumulate_within window expires during outage,
        // a WARN log is emitted during recovery.

        // Step 1: Node 1 becomes leader and processes events
        rulesEngine1.enableLeader();

        // Process first event (t=0, count=0->1)
        String firstEvent = createEvent("{\"sensu\":{\"process\":{\"type\":\"alert\"},\"host\":\"h1\"},\"sequence\":1}");
        String result1 = rulesEngine1.assertEvent(sessionId1, firstEvent);
        assertThat(readValueAsListOfMapOfStringAndObject(result1)).isEmpty();

        // Advance time by 2 seconds (t=2)
        rulesEngine1.advanceTime(sessionId1, 2, "SECONDS");

        // Process second event (t=2, count=1->2)
        String secondEvent = createEvent("{\"sensu\":{\"process\":{\"type\":\"alert\"},\"host\":\"h1\"},\"sequence\":2}");
        String result2 = rulesEngine1.assertEvent(sessionId1, secondEvent);
        assertThat(readValueAsListOfMapOfStringAndObject(result2)).isEmpty();

        // Advance time by 1 more second (t=3)
        rulesEngine1.advanceTime(sessionId1, 1, "SECONDS");

        // Step 2: Simulate Node 1 crash/shutdown
        rulesEngine1.disableLeader();
        rulesEngine1.close();
        rulesEngine1 = null;
        consumer1.stop();
        consumer1 = null;

        // Step 3: Advance Node 2's clock past the window expiration BEFORE recovery
        // Control was created at t=0 with 10s window, so it expires at t=10.
        // Advance node 2 to t=12, simulating the outage lasted longer than the window.
        rulesEngine2.advanceTime(sessionId2, 12, "SECONDS");

        // Step 4: Node 2 takes over — recovery detects the expired accumulate_within control
        // and logs WARN: accumulate_within window expired during outage for rule 'alert_accumulator' ...
        String logs;
        try {
            logs = TestOutputCapture.captureStdout(() -> rulesEngine2.enableLeader());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        assertThat(logs).contains("accumulate_within window expired during outage for rule 'alert_accumulator'");

        // Step 5: Verify that the accumulation was lost (count reset)
        // Processing 1 new event should NOT fire the rule (threshold=3, fresh start)
        String thirdEvent = createEvent("{\"sensu\":{\"process\":{\"type\":\"alert\"},\"host\":\"h1\"},\"sequence\":3}");
        String result3 = rulesEngine2.assertEvent(sessionId2, thirdEvent);
        assertThat(readValueAsListOfMapOfStringAndObject(result3)).isEmpty();
    }

    @Test
    void testSessionRecoveryWithAccumulateWindowExpiration() {
        // This test verifies that the control event expiration is correctly restored across recovery

        // Step 1: Node 1 becomes leader and processes events
        rulesEngine1.enableLeader();

        // Process first event (t=0, count=0->1)
        String firstEvent = createEvent("{\"sensu\":{\"process\":{\"type\":\"alert\"},\"host\":\"h1\"},\"sequence\":1}");
        String result1 = rulesEngine1.assertEvent(sessionId1, firstEvent);

        // Should NOT match - count is 1, need 3
        assertThat(readValueAsListOfMapOfStringAndObject(result1)).isEmpty();

        // Advance time by 3 seconds (t=3)
        rulesEngine1.advanceTime(sessionId1, 3, "SECONDS");

        // Process second event (t=3, count=1->2)
        String secondEvent = createEvent("{\"sensu\":{\"process\":{\"type\":\"alert\"},\"host\":\"h1\"},\"sequence\":2}");
        String result2 = rulesEngine1.assertEvent(sessionId1, secondEvent);

        // Should still NOT match - count is 2, need 3
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
        // recovery happens here, restores control event with current_count=2, advances to t=5
        rulesEngine2.enableLeader();

        // Step 4: Advance time past the window expiration (t=5 + 6 = t=11)
        // Control event expires at t=10 (created at t=0 with 10-second expiration)
        rulesEngine2.advanceTime(sessionId2, 6, "SECONDS");

        // Process third event (t=11)
        // The control event should have expired, so accumulation resets
        String thirdEvent = createEvent("{\"sensu\":{\"process\":{\"type\":\"alert\"},\"host\":\"h1\"},\"sequence\":3}");
        String result3 = rulesEngine2.assertEvent(sessionId2, thirdEvent);

        // Should NOT match - control event expired, count reset to 1
        assertThat(readValueAsListOfMapOfStringAndObject(result3)).isEmpty();
    }
}
