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
 * Integration tests for AstRulesEngine with HA functionality
 */
class HAIntegrationOnceWithinTest extends HAIntegrationTestBase {

    // OnceWithin rule - fires only once within 10 seconds for the same host
    private static final String RULE_SET_ONCE_WITHIN = """
                {
                    "name": "OnceWithin Ruleset",
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
                                    "once_within": "10 seconds"
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
        return RULE_SET_ONCE_WITHIN;
    }

    @Test
    void testSessionRecoveryWithinTimeWindow() {
        // Step 1: Node 1 becomes leader and processes first event
        rulesEngine1.enableLeader();

        // Process first event that matches the rule (t=0)
        String firstEvent = createEvent("{\"alert\":{\"type\":\"warning\",\"host\":\"h1\"}}");
        String result1 = rulesEngine1.assertEvent(sessionId1, firstEvent);

        // Should match since it's the first event for h1
        List<Map<String, Object>> matches1 = readValueAsListOfMapOfStringAndObject(result1);
        assertThat(matches1).hasSize(1);
        assertThat(matches1.get(0))
                .containsEntry("name", "alert_throttle")
                .containsKey("matching_uuid");

        // Advance time by 3 seconds (t=3)
        rulesEngine1.advanceTime(sessionId1, 3, "SECONDS");

        // Process second event for same host within time window
        String secondEvent = createEvent("{\"alert\":{\"type\":\"warning\",\"host\":\"h1\"}}");
        String result2 = rulesEngine1.assertEvent(sessionId1, secondEvent);

        // Should NOT match because still within 10-second window
        assertThat(readValueAsListOfMapOfStringAndObject(result2)).isEmpty();

        // Advance time by 2 more seconds (t=5, still within window)
        // Note: this advance is not persisted in SessionState
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
        rulesEngine2.enableLeader();

        // Step 4: Process third event for same host (t=5, still within window from t=0)
        // The recovered session should maintain the once_within control event
        String thirdEvent = createEvent("{\"alert\":{\"type\":\"warning\",\"host\":\"h1\"}}");
        String result3 = rulesEngine2.assertEvent(sessionId2, thirdEvent);

        // Should still NOT match because still within 10-second window
        assertThat(readValueAsListOfMapOfStringAndObject(result3)).isEmpty();

        // Advance time by 6 more seconds (t=11, outside window)
        rulesEngine2.advanceTime(sessionId2, 6, "SECONDS");

        // Step 5: Process fourth event for same host (t=11, outside window)
        String fourthEvent = createEvent("{\"alert\":{\"type\":\"warning\",\"host\":\"h1\"}}");
        String result4 = rulesEngine2.assertEvent(sessionId2, fourthEvent);

        // Should match again since we're outside the 10-second window
        List<Map<String, Object>> matches4 = readValueAsListOfMapOfStringAndObject(result4);
        assertThat(matches4).hasSize(1);
        assertThat(matches4.get(0))
                .containsEntry("name", "alert_throttle")
                .containsKey("matching_uuid");

        // Step 6: Test different host - should match immediately regardless of window
        String differentHostEvent = createEvent("{\"alert\":{\"type\":\"warning\",\"host\":\"h2\"}}");
        String result5 = rulesEngine2.assertEvent(sessionId2, differentHostEvent);

        List<Map<String, Object>> matches5 = readValueAsListOfMapOfStringAndObject(result5);
        assertThat(matches5).hasSize(1);
        assertThat(matches5.get(0))
                .containsEntry("name", "alert_throttle")
                .containsKey("matching_uuid");
    }

    @Test
    void testOnceWithinExpiryDuringOutageNeedsNoWarning() {
        rulesEngine1.enableLeader();

        // First event starts the suppression window at t=0.
        String firstEvent = createEvent("{\"alert\":{\"type\":\"warning\",\"host\":\"h1\"}}");
        List<Map<String, Object>> firstMatches = readValueAsListOfMapOfStringAndObject(rulesEngine1.assertEvent(sessionId1, firstEvent));
        assertThat(firstMatches).hasSize(1);

        // Advance leader clock to t=3 and persist that state.
        rulesEngine1.advanceTime(sessionId1, 3, "SECONDS");

        // Simulate an outage longer than the 10-second once_within window.
        rulesEngine2.advanceTime(sessionId2, 12, "SECONDS");

        rulesEngine1.disableLeader();
        rulesEngine1.close();
        rulesEngine1 = null;
        consumer1.stop();
        consumer1 = null;

        String logs;
        try {
            logs = TestOutputCapture.captureStdout(() -> rulesEngine2.enableLeader());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        assertThat(logs).doesNotContain("once_within window expired during outage");

        // Suppression window expired during downtime, so the next event should be processed normally.
        String secondEvent = createEvent("{\"alert\":{\"type\":\"warning\",\"host\":\"h1\"}}");
        List<Map<String, Object>> secondMatches = readValueAsListOfMapOfStringAndObject(rulesEngine2.assertEvent(sessionId2, secondEvent));
        assertThat(secondMatches).hasSize(1);
        assertThat(secondMatches.get(0))
                .containsEntry("name", "alert_throttle")
                .containsKey("matching_uuid");
    }
}
