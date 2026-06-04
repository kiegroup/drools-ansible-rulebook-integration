package org.drools.ansible.rulebook.integration.ha.tests.integration.recovery;

import org.drools.ansible.rulebook.integration.ha.tests.support.TestUtils;

import org.drools.ansible.rulebook.integration.ha.tests.support.AbstractHATestBase;

import org.drools.ansible.rulebook.integration.ha.tests.integration.HAIntegrationTestBase;

import java.util.List;
import java.util.Map;

import org.drools.ansible.rulebook.integration.api.RuleConfigurationOption;
import org.drools.ansible.rulebook.integration.api.io.JsonMapper;
import org.drools.ansible.rulebook.integration.core.jpy.AstRulesEngine;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManager;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManagerFactory;
import org.drools.ansible.rulebook.integration.ha.api.HAUtils;
import org.drools.ansible.rulebook.integration.ha.model.SessionState;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.readValueAsListOfMapOfStringAndObject;
import static org.drools.ansible.rulebook.integration.ha.tests.support.TestUtils.createEvent;

/**
 * Tests that HA correctly handles ruleset updates with multi-condition AllCondition rules.
 * When a ruleset is updated, the new session should NOT recover partial events from
 * the old session's SessionState because the rulebook hash has changed.
 */
class HAIntegrationRulesetUpdateMultiConditionTest extends AbstractHATestBase {

    private static final String HA_UUID = "ruleset-update-multi-ha-1";

    static {
        if (USE_POSTGRES) {
            initializePostgres("eda_ha_test", "HA ruleset update multi-condition tests");
        } else {
            initializeH2();
        }
    }

    // V1: requires both i==1 AND j==2
    private static final String RULE_SET_V1 = """
            {
                "name": "Test Ruleset",
                "sources": {"EventSource": "test"},
                "rules": [
                    {"Rule": {
                        "name": "v1_multi_alert",
                        "condition": {
                            "AllCondition": [
                                {
                                    "EqualsExpression": {
                                        "lhs": {"Event": "i"},
                                        "rhs": {"Integer": 1}
                                    }
                                },
                                {
                                    "EqualsExpression": {
                                        "lhs": {"Event": "j"},
                                        "rhs": {"Integer": 2}
                                    }
                                }
                            ]
                        },
                        "action": {
                            "run_playbook": [{"name": "alert_v1.yml"}]
                        }
                    }}
                ]
            }
            """;

    // V2: same ruleset name, but requires k==3 AND l==4
    private static final String RULE_SET_V2 = """
            {
                "name": "Test Ruleset",
                "sources": {"EventSource": "test"},
                "rules": [
                    {"Rule": {
                        "name": "v2_multi_alert",
                        "condition": {
                            "AllCondition": [
                                {
                                    "EqualsExpression": {
                                        "lhs": {"Event": "k"},
                                        "rhs": {"Integer": 3}
                                    }
                                },
                                {
                                    "EqualsExpression": {
                                        "lhs": {"Event": "l"},
                                        "rhs": {"Integer": 4}
                                    }
                                }
                            ]
                        },
                        "action": {
                            "run_playbook": [{"name": "alert_v2.yml"}]
                        }
                    }}
                ]
            }
            """;

    private AstRulesEngine engine1;
    private AstRulesEngine engine2;
    private long sessionId1;
    private long sessionId2;
    private HAIntegrationTestBase.AsyncConsumer consumer1;
    private HAIntegrationTestBase.AsyncConsumer consumer2;

    @AfterEach
    void tearDown() {
        if (consumer1 != null) {
            consumer1.stop();
        }
        if (consumer2 != null) {
            consumer2.stop();
        }
        if (engine1 != null) {
            engine1.close();
        }
        if (engine2 != null) {
            engine2.close();
        }
        cleanupDatabase();
    }

    /**
     * Scenario: Leader node updates its ruleset (multi-condition).
     *
     * 1. Node1 starts with V1 rules (i==1 AND j==2), becomes leader, processes partial match (i=1)
     * 2. Node1 disposes session, creates new session with V2 rules (k==3 AND l==4)
     * 3. V2 should NOT recover V1's partial event (hash mismatch)
     * 4. V1's partial event (i=1) should not interfere with V2
     * 5. V2 rules should work correctly with fresh state
     */
    @Test
    void testUpdateRulesetOnSameNode() {
        // Phase 1: Start with V1 ruleset
        engine1 = new AstRulesEngine();
        engine1.initializeHA(HA_UUID, "worker-1", dbParamsJson, dbHAConfigJson);
        sessionId1 = engine1.createRuleset(RULE_SET_V1, RuleConfigurationOption.FULLY_MANUAL_PSEUDOCLOCK);

        consumer1 = new HAIntegrationTestBase.AsyncConsumer("consumer1");
        consumer1.startConsuming(engine1.port());

        engine1.enableLeader();

        // Process first event (partial match for V1: i=1, still needs j=2)
        String event_i = createEvent("{\"i\": 1}");
        String result1 = engine1.assertEvent(sessionId1, event_i);
        assertThat(readValueAsListOfMapOfStringAndObject(result1)).isEmpty();

        // Verify partial state is persisted
        HAStateManager assertionManager = createHAStateManagerForAssertion();
        try {
            SessionState stateV1 = assertionManager.getPersistedSessionState("Test Ruleset");
            assertThat(stateV1).isNotNull();
            assertThat(stateV1.getRulebookHash()).isNotNull();
            // V1 has a partial event in SessionState
            assertThat(stateV1.getPartialEvents()).isNotEmpty();
            String v1Hash = stateV1.getRulebookHash();

            // Phase 2: Dispose and recreate with V2 ruleset (same name)
            engine1.dispose(sessionId1);
            sessionId1 = engine1.createRuleset(RULE_SET_V2, RuleConfigurationOption.FULLY_MANUAL_PSEUDOCLOCK);

            // Verify the new session state has a different hash
            SessionState stateV2 = assertionManager.getPersistedSessionState("Test Ruleset");
            assertThat(stateV2).isNotNull();
            assertThat(stateV2.getRulebookHash()).isNotEqualTo(v1Hash);
            // V2 should have empty partial events (V1's partial event was not recovered)
            assertThat(stateV2.getPartialEvents()).isEmpty();

            // Phase 3: Verify V2 rules work correctly with fresh state
            // Sending j=2 (V1's completing event) should NOT trigger a match
            String event_j = createEvent("{\"j\": 2}");
            String result2 = engine1.assertEvent(sessionId1, event_j);
            assertThat(readValueAsListOfMapOfStringAndObject(result2)).isEmpty();

            // V2 partial match: k=3 (still needs l=4)
            String event_k = createEvent("{\"k\": 3}");
            String result3 = engine1.assertEvent(sessionId1, event_k);
            assertThat(readValueAsListOfMapOfStringAndObject(result3)).isEmpty();

            // V2 completing match: l=4
            String event_l = createEvent("{\"l\": 4}");
            String result4 = engine1.assertEvent(sessionId1, event_l);
            List<Map<String, Object>> matches = readValueAsListOfMapOfStringAndObject(result4);
            assertThat(matches).hasSize(1);
            assertThat(matches.get(0)).containsEntry("name", "v2_multi_alert");
        } finally {
            assertionManager.shutdown();
        }
    }

    /**
     * Scenario: Node2 has updated ruleset and takes over as leader after failover (multi-condition).
     *
     * 1. Node1 starts with V1 rules (i==1 AND j==2), becomes leader, processes partial match (i=1)
     * 2. Node2 starts with V2 rules (k==3 AND l==4)
     * 3. Node1 fails, Node2 becomes leader
     * 4. Node2 should detect hash mismatch and NOT recover V1's partial event
     * 5. Node2's V2 rules should work correctly with fresh state
     */
    @Test
    void testUpdateRulesetOnNode2AndFailover() {
        // Phase 1: Node1 with V1 ruleset as leader
        engine1 = new AstRulesEngine();
        engine1.initializeHA(HA_UUID, "worker-1", dbParamsJson, dbHAConfigJson);
        sessionId1 = engine1.createRuleset(RULE_SET_V1, RuleConfigurationOption.FULLY_MANUAL_PSEUDOCLOCK);

        consumer1 = new HAIntegrationTestBase.AsyncConsumer("consumer1");
        consumer1.startConsuming(engine1.port());

        engine1.enableLeader();

        // Process partial match with V1: i=1 (still needs j=2)
        String event_i = createEvent("{\"i\": 1}");
        String result1 = engine1.assertEvent(sessionId1, event_i);
        assertThat(readValueAsListOfMapOfStringAndObject(result1)).isEmpty();

        // Phase 2: Node2 starts with V2 ruleset (same name "Test Ruleset")
        engine2 = new AstRulesEngine();
        engine2.initializeHA(HA_UUID, "worker-2", dbParamsJson, dbHAConfigJson);
        sessionId2 = engine2.createRuleset(RULE_SET_V2, RuleConfigurationOption.FULLY_MANUAL_PSEUDOCLOCK);

        consumer2 = new HAIntegrationTestBase.AsyncConsumer("consumer2");
        consumer2.startConsuming(engine2.port());

        // Phase 3: Failover - Node1 goes down, Node2 becomes leader
        engine1.disableLeader();
        engine2.enableLeader();

        // Phase 4: Verify Node2 does NOT have V1's partial event
        // Sending j=2 (V1's completing event) should NOT trigger a match on Node2
        String event_j = createEvent("{\"j\": 2}");
        String result2 = engine2.assertEvent(sessionId2, event_j);
        assertThat(readValueAsListOfMapOfStringAndObject(result2)).isEmpty();

        // Phase 5: Verify V2 rules work correctly with fresh state
        // V2 partial match: k=3
        String event_k = createEvent("{\"k\": 3}");
        String result3 = engine2.assertEvent(sessionId2, event_k);
        assertThat(readValueAsListOfMapOfStringAndObject(result3)).isEmpty();

        // V2 completing match: l=4
        String event_l = createEvent("{\"l\": 4}");
        String result4 = engine2.assertEvent(sessionId2, event_l);
        List<Map<String, Object>> matches = readValueAsListOfMapOfStringAndObject(result4);
        assertThat(matches).hasSize(1);
        assertThat(matches.get(0)).containsEntry("name", "v2_multi_alert");

        // Verify fresh state is persisted with V2 hash
        HAStateManager assertionManager = createHAStateManagerForAssertion();
        try {
            SessionState state = assertionManager.getPersistedSessionState("Test Ruleset");
            assertThat(state).isNotNull();
            String v1Hash = HAUtils.sha256(RULE_SET_V1);
            String v2Hash = HAUtils.sha256(RULE_SET_V2);
            assertThat(state.getRulebookHash()).isNotEqualTo(v1Hash);
            assertThat(state.getRulebookHash()).isEqualTo(v2Hash);
        } finally {
            assertionManager.shutdown();
        }
    }

    private HAStateManager createHAStateManagerForAssertion() {
        HAStateManager manager = HAStateManagerFactory.create(TEST_DB_TYPE);
        manager.initializeHA(HA_UUID, "FOR_ASSERTION", dbParams, dbHAConfig);
        return manager;
    }
}
