package org.drools.ansible.rulebook.integration.ha.tests;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.readValueAsListOfMapOfStringAndObject;
import static org.drools.ansible.rulebook.integration.ha.tests.TestUtils.createEvent;

/**
 * HA integration tests for match_multiple_rules with partial match retention.
 * With match_multiple_rules enabled, partial matches from a fired rule are retained
 * so that peer rules with additional conditions can still use them.
 * This test verifies that retained partial matches survive failover recovery.
 *
 * Ruleset: R1 matches on i==1 (single condition),
 *          R2 matches on i==1 AND j==2 (multi-condition).
 * Event {i:1} fires R1 and creates a partial match for R2.
 * After failover, event {j:2} should complete R2 on the recovered node.
 */
class HAIntegrationMatchMultipleRulesPartialMatchTest extends HAIntegrationTestBase {

    private static final String RULE_SET = """
                {
                    "name": "Match Multiple Partial Ruleset",
                    "match_multiple_rules": true,
                    "rules": [
                        {"Rule": {
                            "name": "R1",
                            "condition": {
                                "EqualsExpression": {
                                    "lhs": {
                                        "Event": "i"
                                    },
                                    "rhs": {
                                        "Integer": 1
                                    }
                                }
                            },
                            "action": {
                                "run_playbook": [
                                    {
                                        "name": "r1_playbook.yml"
                                    }
                                ]
                            }
                        }},
                        {"Rule": {
                            "name": "R2",
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
                                    }
                                ]
                            },
                            "action": {
                                "run_playbook": [
                                    {
                                        "name": "r2_playbook.yml"
                                    }
                                ]
                            }
                        }}
                    ]
                }
                """;

    @Override
    protected String getRuleSet() {
        return RULE_SET;
    }

    /**
     * Event {i:1} fires R1 (single condition) and creates a partial match for R2.
     * Then event {j:2} completes R2 on the same node.
     */
    @Test
    void testPartialMatchRetainedForMultiConditionRule() {
        rulesEngine1.enableLeader();

        // Event {i:1} fires R1, creates partial match for R2
        String event1 = createEvent("{\"i\":1}");
        String result1 = rulesEngine1.assertEvent(sessionId1, event1);

        List<Map<String, Object>> matches1 = readValueAsListOfMapOfStringAndObject(result1);
        assertThat(matches1).hasSize(1);
        assertThat(matches1.get(0)).containsEntry("name", "R1");

        // Event {j:2} completes R2 using retained partial match (i=1)
        String event2 = createEvent("{\"j\":2}");
        String result2 = rulesEngine1.assertEvent(sessionId1, event2);

        List<Map<String, Object>> matches2 = readValueAsListOfMapOfStringAndObject(result2);
        assertThat(matches2).hasSize(1);
        assertThat(matches2.get(0)).containsEntry("name", "R2");
    }

    /**
     * Event {i:1} fires R1 on node 1 and creates a partial match for R2.
     * After failover, event {j:2} on node 2 should complete R2 using the recovered partial match.
     */
    @Test
    void testPartialMatchRetainedAcrossFailover() {
        // Node 1 becomes leader
        rulesEngine1.enableLeader();

        // Event {i:1} fires R1, creates partial match for R2
        String event1 = createEvent("{\"i\":1}");
        String result1 = rulesEngine1.assertEvent(sessionId1, event1);

        List<Map<String, Object>> matches1 = readValueAsListOfMapOfStringAndObject(result1);
        assertThat(matches1).hasSize(1);
        assertThat(matches1.get(0)).containsEntry("name", "R1");

        // Simulate Node 1 crash
        rulesEngine1.disableLeader();
        rulesEngine1.close();
        rulesEngine1 = null;
        consumer1.stop();
        consumer1 = null;

        // Node 2 recovers and takes over
        rulesEngine2.enableLeader();

        // Event {j:2} should complete R2 using recovered partial match (i=1)
        String event2 = createEvent("{\"j\":2}");
        String result2 = rulesEngine2.assertEvent(sessionId2, event2);

        List<Map<String, Object>> matches2 = readValueAsListOfMapOfStringAndObject(result2);
        assertThat(matches2).hasSize(1);
        assertThat(matches2.get(0)).containsEntry("name", "R2");
    }
}
