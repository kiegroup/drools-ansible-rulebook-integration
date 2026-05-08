package org.drools.ansible.rulebook.integration.ha.tests;

import java.util.List;
import java.util.Map;

import org.drools.ansible.rulebook.integration.core.jpy.AstRulesEngine;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.readValueAsListOfMapOfStringAndObject;
import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.readValueAsMapOfStringAndObject;
import static org.drools.ansible.rulebook.integration.ha.tests.TestUtils.createEvent;

/**
 * Integration tests for AstRulesEngine with HA functionality
 */
class HAIntegration3ConditionTest extends HAIntegrationTestBase {

    // Multi condition rule
    private static final String RULE_SET_3_CONDITION = """
                {
                    "name": "Multi Condition Ruleset",
                    "rules": [
                        {"Rule": {
                            "name": "temperature_alert",
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
                                    },
                                    {
                                        "EqualsExpression": {
                                            "lhs": {
                                                "Event": "j"
                                            },
                                            "rhs": {
                                                "Integer": 2
                                            }
                                        }
                                    },
                                    {
                                        "EqualsExpression": {
                                            "lhs": {
                                                "Event": "k"
                                            },
                                            "rhs": {
                                                "Integer": 3
                                            }
                                        }
                                    }
                                ]
                            },
                            "action": {
                                "run_playbook": [
                                    {
                                        "name": "send_alert.yml",
                                        "extra_vars": {
                                            "message": "High temperature detected"
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
        return RULE_SET_3_CONDITION;
    }

    @Test
    void testSessionRecoveryWithPartialMatch() {
        // Step 1: Node 1 becomes leader and processes first event (partial match)
        rulesEngine1.enableLeader();

        // Process first event that creates partial match
        String firstEvent = createEvent("{\"i\":1}");
        String result1 = rulesEngine1.assertEvent(sessionId1, firstEvent);

        // Should be empty since rule requires i=1 AND j=2 AND k=3
        assertThat(readValueAsListOfMapOfStringAndObject(result1)).isEmpty();

        // Process second event that creates partial match
        String secondEvent = createEvent("{\"j\":2}");
        String result2 = rulesEngine1.assertEvent(sessionId1, secondEvent);

        // Should be empty since rule requires i=1 AND j=2 AND k=3
        assertThat(readValueAsListOfMapOfStringAndObject(result2)).isEmpty();

        String haStatsJson = rulesEngine1.getHAStats();
        Map<String, Object> haStats = readValueAsMapOfStringAndObject(haStatsJson);
        assertThat(haStats).containsEntry("partial_events_in_memory", 2);
        assertThat(haStats).containsEntry("partial_fulfilled_rules", 1);

        // Advance time to simulate processing delay
        rulesEngine1.advanceTime(sessionId1, 5, "SECONDS");

        // Step 2: Simulate Node 1 crash/shutdown
        rulesEngine1.disableLeader();
        rulesEngine1.close();
        rulesEngine1 = null;
        consumer1.stop();
        consumer1 = null;

        // Step 3: Node 2 takes over and recovers session
        rulesEngine2.enableLeader();

        // Step 4: Node 2 processes third event that should complete the match
        // The recovered session should have the partial match from the first event and second event
        String thirdEvent = createEvent("{\"k\":3}");
        String result3 = rulesEngine2.assertEvent(sessionId2, thirdEvent);

        // Should now have a complete match since all conditions are satisfied
        // (i=1, j=2 from recovered state + k=3 from current event)
        List<Map<String, Object>> matches = readValueAsListOfMapOfStringAndObject(result3);
        assertThat(matches).hasSize(1);

        Map<String, Object> match = matches.get(0);
        assertThat(match).containsEntry("name", "temperature_alert")
                .containsKey("matching_uuid");
    }
}
