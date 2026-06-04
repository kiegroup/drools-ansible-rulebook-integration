package org.drools.ansible.rulebook.integration.ha.tests.integration.basic;

import org.drools.ansible.rulebook.integration.ha.tests.support.TestUtils;

import org.drools.ansible.rulebook.integration.ha.tests.integration.HAIntegrationTestBase;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.readValueAsListOfMapOfStringAndObject;
import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.readValueAsMapOfStringAndObject;
import static org.drools.ansible.rulebook.integration.ha.tests.support.TestUtils.createEvent;

/**
 * Integration tests for AstRulesEngine with HA functionality
 */
class HAIntegrationMultiConditionFactTest extends HAIntegrationTestBase {

    // Multi condition rule
    private static final String RULE_SET_MULTI_CONDITION_FACT = """
                {
                    "name": "Multi Condition Fact Ruleset",
                    "rules": [
                        {"Rule": {
                            "name": "temperature_alert",
                            "condition": {
                                "AllCondition": [
                                    {
                                        "EqualsExpression": {
                                            "lhs": {
                                                "Fact": "i"
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
        return RULE_SET_MULTI_CONDITION_FACT;
    }

    @Test
    void testSessionRecoveryWithPartialMatch() {
        // Step 1: Node 1 becomes leader and processes first fact (partial match)
        rulesEngine1.enableLeader();

        // Process first fact that creates partial match
        String firstFact = "{\"i\":1}";
        String result1 = rulesEngine1.assertFact(sessionId1, firstFact);

        // Should be empty since rule requires both i=1 AND j=2
        assertThat(readValueAsListOfMapOfStringAndObject(result1)).isEmpty();

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

        // Step 4: Node 2 processes second event that should complete the match
        // The recovered session should have the partial match from the first fact
        String secondEvent = createEvent("{\"j\":2}");
        String result2 = rulesEngine2.assertEvent(sessionId2, secondEvent);

        // Should now have a complete match since both conditions are satisfied
        // (i=1 from recovered state + j=2 from current event)
        List<Map<String, Object>> matches = readValueAsListOfMapOfStringAndObject(result2);
        assertThat(matches).hasSize(1);

        Map<String, Object> match = matches.get(0);
        assertThat(match).containsEntry("name", "temperature_alert")
                .containsKey("matching_uuid");
    }

    @Test
    void testHaStatsIncrementOnFactAssertion() {
        rulesEngine1.enableLeader();

        String statsJson = rulesEngine1.getHAStats();
        Map<String, Object> statsAfterLeader = readValueAsMapOfStringAndObject(statsJson);
        assertThat(statsAfterLeader.get("current_leader")).isEqualTo("worker-1");
        assertThat(((Number) statsAfterLeader.get("events_processed_in_term")).intValue()).isZero();

        rulesEngine1.assertFact(sessionId1, "{\"i\":1}");

        statsJson = rulesEngine1.getHAStats();
        Map<String, Object> statsAfterFact = readValueAsMapOfStringAndObject(statsJson);
        assertThat(((Number) statsAfterFact.get("events_processed_in_term")).intValue()).isEqualTo(1);
    }
}
