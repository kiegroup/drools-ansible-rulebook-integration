package org.drools.ansible.rulebook.integration.ha.tests;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.readValueAsListOfMapOfStringAndObject;
import static org.drools.ansible.rulebook.integration.ha.tests.TestUtils.createEvent;

/**
 * HA integration tests for match_multiple_rules functionality.
 * Verifies that when match_multiple_rules is enabled, a single event can fire
 * multiple rules, and that this behavior is preserved across failover recovery.
 */
class HAIntegrationMatchMultipleRulesTest extends HAIntegrationTestBase {

    // Two rules that match the same condition (i == 1), with match_multiple_rules enabled.
    private static final String RULE_SET = """
                {
                    "name": "Match Multiple Rules Ruleset",
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
     * A single event should fire both R1 and R2 when match_multiple_rules is true.
     */
    @Test
    void testSingleEventMatchesMultipleRules() {
        rulesEngine1.enableLeader();

        String event = createEvent("{\"i\":1}");
        String result = rulesEngine1.assertEvent(sessionId1, event);

        List<Map<String, Object>> matches = readValueAsListOfMapOfStringAndObject(result);
        assertThat(matches).hasSize(2);
        assertThat(matches).extracting(m -> m.get("name"))
                .containsExactlyInAnyOrder("R1", "R2");
    }

    /**
     * A non-matching event should fire no rules.
     */
    @Test
    void testNonMatchingEventFiresNoRules() {
        rulesEngine1.enableLeader();

        String event = createEvent("{\"i\":99}");
        String result = rulesEngine1.assertEvent(sessionId1, event);

        assertThat(readValueAsListOfMapOfStringAndObject(result)).isEmpty();
    }

    /**
     * After failover, a new event on node 2 should still fire multiple rules.
     */
    @Test
    void testMatchMultipleRulesAfterFailover() {
        // Node 1 processes an event (both rules fire)
        rulesEngine1.enableLeader();

        String event1 = createEvent("{\"i\":1}");
        String result1 = rulesEngine1.assertEvent(sessionId1, event1);
        assertThat(readValueAsListOfMapOfStringAndObject(result1)).hasSize(2);

        // Simulate Node 1 crash
        rulesEngine1.disableLeader();
        rulesEngine1.close();
        rulesEngine1 = null;
        consumer1.stop();
        consumer1 = null;

        // Node 2 takes over
        rulesEngine2.enableLeader();

        // New event on node 2 should still match multiple rules
        String event2 = createEvent("{\"i\":1}");
        String result2 = rulesEngine2.assertEvent(sessionId2, event2);

        List<Map<String, Object>> matches = readValueAsListOfMapOfStringAndObject(result2);
        assertThat(matches).hasSize(2);
        assertThat(matches).extracting(m -> m.get("name"))
                .containsExactlyInAnyOrder("R1", "R2");
    }
}
