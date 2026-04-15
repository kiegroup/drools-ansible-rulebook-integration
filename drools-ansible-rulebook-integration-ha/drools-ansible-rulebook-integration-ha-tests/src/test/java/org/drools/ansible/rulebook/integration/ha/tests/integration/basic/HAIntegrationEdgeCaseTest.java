package org.drools.ansible.rulebook.integration.ha.tests.integration.basic;

import org.drools.ansible.rulebook.integration.ha.tests.support.TestUtils;

import org.drools.ansible.rulebook.integration.ha.tests.integration.HAIntegrationTestBase;

import org.drools.ansible.rulebook.integration.api.RuleConfigurationOption;
import org.drools.ansible.rulebook.integration.core.jpy.AstRulesEngine;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.drools.ansible.rulebook.integration.ha.tests.support.TestUtils.createEvent;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Integration tests for AstRulesEngine with HA functionality
 */
class HAIntegrationEdgeCaseTest extends HAIntegrationTestBase {

    // Basic rule
    private static final String RULE_SET_BASIC = """
                {
                    "name": "Test Ruleset",
                    "sources": {"EventSource": "test"},
                    "rules": [
                        {"Rule": {
                            "name": "temperature_alert",
                            "condition": {
                                "GreaterThanExpression": {
                                    "lhs": {
                                        "Event": "temperature"
                                    },
                                    "rhs": {
                                        "Integer": 30
                                    }
                                }
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

    protected String getRuleSet() {
        return RULE_SET_BASIC;
    }

    @BeforeEach
    @Override
    protected void setUp() {
        System.out.println("Running test with database: " + TEST_DB_TYPE);

        rulesEngine1 = new AstRulesEngine();
        rulesEngine1.initializeHA(HA_UUID, "worker-1", dbParamsJson, dbHAConfigJson); // The same cluster. Both nodes share same DB
        // This test doesn't create ruleset here, because some tests need to create ruleset after becoming leader

        consumer1 = new AsyncConsumer("consumer1");
        consumer1.startConsuming(rulesEngine1.port());
    }

    @Test
    void testCurrentStateSha_createRulesetAfterBecomingLeader() {
        // Scenario: Become leader before Creating ruleset => Exception
        assertThrows(IllegalStateException.class, () -> {
            rulesEngine1.enableLeader();
        });
    }

    // This is a little tricky scenario. Usually we expect a leader is taken over by another node.
    // But this HA implementation allows the same node to restart the engine process and become leader again.
    // Not a real requirement, but probably good to keep this capability.
    @Test
    void testSingleRestart() {
        sessionId1 = rulesEngine1.createRuleset(getRuleSet(), RuleConfigurationOption.FULLY_MANUAL_PSEUDOCLOCK);
        rulesEngine1.enableLeader();

        // Process an event
        String event = createEvent("""
                {
                    "temperature": 45,
                    "critical": true
                }
                """);
        String result = rulesEngine1.assertEvent(sessionId1, event);
        assertThat(result).contains("temperature_alert");

        // Simulate engine-1 crash
        rulesEngine1 = null;
        consumer1.stop();
        consumer1 = null;

        // Simulate restarting engine-1 on the same node. The old instance is gone, so we create a new one
        AstRulesEngine rulesEngine1Restart = new AstRulesEngine();
        rulesEngine1Restart.initializeHA(HA_UUID, "worker-1", dbParamsJson, dbHAConfigJson);
        long sessionId1Restart = rulesEngine1Restart.createRuleset(getRuleSet(), RuleConfigurationOption.FULLY_MANUAL_PSEUDOCLOCK);
        AsyncConsumer consumer1restart = new AsyncConsumer("consumer1-restart");
        consumer1restart.startConsuming(rulesEngine1Restart.port());

        rulesEngine1Restart.enableLeader();

        // Process another event
        String event2 = createEvent("""
                {
                    "temperature": 50,
                    "critical": true
                }
                """);
        String result2 = rulesEngine1Restart.assertEvent(sessionId1Restart, event2);
        assertThat(result2).contains("temperature_alert");

        consumer1restart.stop();
    }
}
