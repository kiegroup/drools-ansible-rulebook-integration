package org.drools.ansible.rulebook.integration.ha.tests.integration.basic;

import org.drools.ansible.rulebook.integration.ha.tests.integration.HAIntegrationTestBase;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.readValueAsListOfMapOfStringAndObject;

/**
 * Integration test for retractMatchingFacts with HA functionality
 */
class HAIntegrationRetractTest extends HAIntegrationTestBase {

    // Rule with IsNotDefinedExpression - fires when fact "i" is NOT present
    private static final String RULE_SET_IS_NOT_DEFINED = """
                {
                    "name": "IsNotDefined Ruleset",
                    "rules": [
                        {"Rule": {
                            "name": "alert_when_sensor_absent",
                            "condition": {
                                "IsNotDefinedExpression": {
                                    "Event": "i"
                                }
                            },
                            "action": {
                                "run_playbook": [
                                    {
                                        "name": "alert.yml",
                                        "extra_vars": {
                                            "message": "Sensor data is absent"
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
        return RULE_SET_IS_NOT_DEFINED;
    }

    @Test
    void testRetractTriggersMatchAfterFailover() {
        // Step 1: Node 1 becomes leader and inserts fact - should NOT match
        // (Rule fires when "i" is absent, but we're inserting it, so no match)
        rulesEngine1.enableLeader();

        String fact = "{\"i\":1}";
        String result1 = rulesEngine1.assertFact(sessionId1, fact);

        // Should be empty since fact "i" is now present (rule needs it absent)
        assertThat(readValueAsListOfMapOfStringAndObject(result1)).isEmpty();

        // Advance time to simulate processing delay
        rulesEngine1.advanceTime(sessionId1, 5, "SECONDS");

        // Step 2: Simulate Node 1 crash/shutdown (fail-over)
        rulesEngine1.disableLeader();
        rulesEngine1.close();
        rulesEngine1 = null;
        consumer1.stop();
        consumer1 = null;

        // Node 2 takes over as leader
        rulesEngine2.enableLeader();

        // Step 3: Node 2 retracts the fact - should NOW match
        // (Retracting "i" makes it absent, triggering IsNotDefinedExpression)
        String result2 = rulesEngine2.retractMatchingFacts(sessionId2, fact, false);

        // Should now have a match since fact "i" is absent after retraction
        List<Map<String, Object>> matches = readValueAsListOfMapOfStringAndObject(result2);
        assertThat(matches).hasSize(1);

        Map<String, Object> match = matches.get(0);
        assertThat(match).containsEntry("name", "alert_when_sensor_absent")
                .containsKey("matching_uuid");
    }
}
