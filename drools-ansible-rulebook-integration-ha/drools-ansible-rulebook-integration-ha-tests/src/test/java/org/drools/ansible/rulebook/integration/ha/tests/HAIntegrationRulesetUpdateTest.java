package org.drools.ansible.rulebook.integration.ha.tests;

import java.util.List;
import java.util.Map;

import org.drools.ansible.rulebook.integration.api.RuleConfigurationOption;
import org.drools.ansible.rulebook.integration.api.io.JsonMapper;
import org.drools.ansible.rulebook.integration.core.jpy.AstRulesEngine;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManager;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManagerFactory;
import org.drools.ansible.rulebook.integration.ha.model.SessionState;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.toJson;
import static org.drools.ansible.rulebook.integration.ha.tests.TestUtils.createEvent;

/**
 * Tests that HA correctly handles ruleset updates by detecting rulebook hash mismatch
 * and creating a fresh session instead of recovering stale state.
 */
class HAIntegrationRulesetUpdateTest extends AbstractHATestBase {

    private static final String HA_UUID = "ruleset-update-ha-1";

    static {
        if (USE_POSTGRES) {
            initializePostgres("eda_ha_test", "HA ruleset update tests");
        } else {
            initializeH2();
        }
    }

    // V1_MULTI: requires BOTH temperature > 30 AND humidity > 50 to fire.
    // Sending only a temperature event creates a partial match (event stays in working memory).
    private static final String RULE_SET_V1_MULTI = """
            {
                "name": "Test Ruleset",
                "sources": {"EventSource": "test"},
                "rules": [
                    {"Rule": {
                        "name": "temp_and_humidity_alert",
                        "condition": {
                            "AllCondition": [
                                {
                                    "GreaterThanExpression": {
                                        "lhs": {"Event": "temperature"},
                                        "rhs": {"Integer": 30}
                                    }
                                },
                                {
                                    "GreaterThanExpression": {
                                        "lhs": {"Event": "humidity"},
                                        "rhs": {"Integer": 50}
                                    }
                                }
                            ]
                        },
                        "action": {
                            "run_playbook": [{"name": "alert_multi.yml"}]
                        }
                    }}
                ]
            }
            """;

    // V2_MULTI: requires BOTH temperature > 30 AND pressure > 100 to fire.
    // Shares the temperature condition with V1_MULTI, so a recovered temperature partial event
    // can complete the match when a pressure event arrives.
    private static final String RULE_SET_V2_MULTI = """
            {
                "name": "Test Ruleset",
                "sources": {"EventSource": "test"},
                "rules": [
                    {"Rule": {
                        "name": "temp_and_pressure_alert",
                        "condition": {
                            "AllCondition": [
                                {
                                    "GreaterThanExpression": {
                                        "lhs": {"Event": "temperature"},
                                        "rhs": {"Integer": 30}
                                    }
                                },
                                {
                                    "GreaterThanExpression": {
                                        "lhs": {"Event": "pressure"},
                                        "rhs": {"Integer": 100}
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

    // V_PRESSURE: completely unrelated domain - triggers on pressure > 100 (single condition).
    // Used to test that stale partial events from V1_MULTI are naturally cleaned up during recovery.
    private static final String RULE_SET_V_PRESSURE = """
            {
                "name": "Test Ruleset",
                "sources": {"EventSource": "test"},
                "rules": [
                    {"Rule": {
                        "name": "pressure_alert",
                        "condition": {
                            "GreaterThanExpression": {
                                "lhs": {"Event": "pressure"},
                                "rhs": {"Integer": 100}
                            }
                        },
                        "action": {
                            "run_playbook": [{"name": "alert_pressure.yml"}]
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
     * Scenario: Leader node updates its ruleset (overwrite_if_rulebook_changes=true, default).
     *
     * 1. Node1 starts with V1_MULTI rules, becomes leader, sends temperature event (partial match)
     * 2. Node1 disposes session, creates new session with V2_MULTI rules
     * 3. V2_MULTI should NOT recover V1's partial events (hash mismatch → old state deleted)
     * 4. Sending only pressure event should NOT fire (temperature partial was not recovered)
     */
    @Test
    void testUpdateRulesetOnSameNode() {
        // Phase 1: Start with V1_MULTI ruleset
        engine1 = new AstRulesEngine();
        engine1.initializeHA(HA_UUID, "worker-1", dbParamsJson, dbHAConfigJson);
        sessionId1 = engine1.createRuleset(RULE_SET_V1_MULTI, RuleConfigurationOption.FULLY_MANUAL_PSEUDOCLOCK);

        consumer1 = new HAIntegrationTestBase.AsyncConsumer("consumer1");
        consumer1.startConsuming(engine1.port());

        engine1.enableLeader();

        // Send only a temperature event — partial match (waiting for humidity)
        String tempEvent = createEvent("{\"temperature\": 35}");
        String result1 = engine1.assertEvent(sessionId1, tempEvent);
        List<Map<String, Object>> matchList1 = JsonMapper.readValueAsListOfMapOfStringAndObject(result1);
        assertThat(matchList1).isEmpty();

        // Verify partial event is persisted
        HAStateManager assertionManager = createHAStateManagerForAssertion();
        try {
            SessionState stateV1 = assertionManager.getPersistedSessionState("Test Ruleset");
            assertThat(stateV1).isNotNull();
            assertThat(stateV1.getPartialEvents())
                    .as("V1 should have a partial event (temperature waiting for humidity)")
                    .isNotEmpty();
            String v1Hash = stateV1.getRulebookHash();

            // Phase 2: Dispose and recreate with V2_MULTI ruleset (same name)
            engine1.dispose(sessionId1);
            sessionId1 = engine1.createRuleset(RULE_SET_V2_MULTI, RuleConfigurationOption.FULLY_MANUAL_PSEUDOCLOCK);

            // Verify the new session state has a different hash and no partial events
            SessionState stateV2 = assertionManager.getPersistedSessionState("Test Ruleset");
            assertThat(stateV2).isNotNull();
            assertThat(stateV2.getRulebookHash()).isNotEqualTo(v1Hash);
            assertThat(stateV2.getPartialEvents())
                    .as("Old partial events should be deleted on overwrite")
                    .isEmpty();

            // Phase 3: Sending only pressure should NOT fire (temperature partial was not recovered)
            String pressureEvent = createEvent("{\"pressure\": 150}");
            String result2 = engine1.assertEvent(sessionId1, pressureEvent);
            List<Map<String, Object>> matchList2 = JsonMapper.readValueAsListOfMapOfStringAndObject(result2);
            assertThat(matchList2)
                    .as("Rule should NOT fire because temperature partial event was not recovered")
                    .isEmpty();
        } finally {
            assertionManager.shutdown();
        }
    }

    /**
     * Scenario: Node2 has updated ruleset and takes over as leader after failover (overwrite=true, default).
     *
     * 1. Node1 starts with V1_MULTI rules, becomes leader, sends temperature event (partial match)
     * 2. Node2 starts with V2_MULTI rules (same name, different conditions)
     * 3. Node1 fails, Node2 becomes leader
     * 4. Node2 should detect hash mismatch, delete old state, and NOT recover partial events
     * 5. Sending only pressure event should NOT fire (temperature partial was not recovered)
     */
    @Test
    void testUpdateRulesetOnNode2AndFailover() {
        // Phase 1: Node1 with V1_MULTI ruleset as leader
        engine1 = new AstRulesEngine();
        engine1.initializeHA(HA_UUID, "worker-1", dbParamsJson, dbHAConfigJson);
        sessionId1 = engine1.createRuleset(RULE_SET_V1_MULTI, RuleConfigurationOption.FULLY_MANUAL_PSEUDOCLOCK);

        consumer1 = new HAIntegrationTestBase.AsyncConsumer("consumer1");
        consumer1.startConsuming(engine1.port());

        engine1.enableLeader();

        // Send only a temperature event — partial match (waiting for humidity)
        String tempEvent = createEvent("{\"temperature\": 35}");
        String result1 = engine1.assertEvent(sessionId1, tempEvent);
        List<Map<String, Object>> matchList1 = JsonMapper.readValueAsListOfMapOfStringAndObject(result1);
        assertThat(matchList1).isEmpty();

        // Phase 2: Node2 starts with V2_MULTI ruleset (same name "Test Ruleset")
        engine2 = new AstRulesEngine();
        engine2.initializeHA(HA_UUID, "worker-2", dbParamsJson, dbHAConfigJson);
        sessionId2 = engine2.createRuleset(RULE_SET_V2_MULTI, RuleConfigurationOption.FULLY_MANUAL_PSEUDOCLOCK);

        consumer2 = new HAIntegrationTestBase.AsyncConsumer("consumer2");
        consumer2.startConsuming(engine2.port());

        // Phase 3: Failover - Node1 goes down, Node2 becomes leader
        engine1.disableLeader();
        engine2.enableLeader();

        // Phase 4: Verify partial events were NOT recovered (overwrite=true deleted old state)
        HAStateManager assertionManager = createHAStateManagerForAssertion();
        try {
            SessionState state = assertionManager.getPersistedSessionState("Test Ruleset");
            assertThat(state).isNotNull();
            String v1Hash = org.drools.ansible.rulebook.integration.ha.api.HAUtils.sha256(RULE_SET_V1_MULTI);
            String v2Hash = org.drools.ansible.rulebook.integration.ha.api.HAUtils.sha256(RULE_SET_V2_MULTI);
            assertThat(state.getRulebookHash()).isNotEqualTo(v1Hash);
            assertThat(state.getRulebookHash()).isEqualTo(v2Hash);
            assertThat(state.getPartialEvents())
                    .as("Old partial events should be deleted on overwrite")
                    .isEmpty();
        } finally {
            assertionManager.shutdown();
        }

        // Phase 5: Sending only pressure should NOT fire (temperature partial was not recovered)
        String pressureEvent = createEvent("{\"pressure\": 150}");
        String result2 = engine2.assertEvent(sessionId2, pressureEvent);
        List<Map<String, Object>> matchList2 = JsonMapper.readValueAsListOfMapOfStringAndObject(result2);
        assertThat(matchList2)
                .as("Rule should NOT fire because temperature partial event was not recovered")
                .isEmpty();
    }

    // Config JSON with overwrite_if_rulebook_changes disabled (keep old persisted data despite mismatch)
    private static final String dbHAConfigJsonWithNoOverwrite = toJson(Map.of("write_after", 1, "overwrite_if_rulebook_changes", false));

    /**
     * Scenario: Leader node updates its ruleset with overwrite_if_rulebook_changes=false.
     *
     * 1. Node1 starts with V1_MULTI rules, becomes leader, sends temperature event (partial match)
     * 2. Node1 disposes session, creates new session with V2_MULTI rules (overwrite_if_rulebook_changes=false)
     * 3. V2_MULTI should recover partial events from V1 despite hash mismatch
     * 4. Sending pressure event should fire V2_MULTI rule (recovered temperature + new pressure)
     */
    @Test
    void testUpdateRulesetWithNoOverwrite() {
        // Phase 1: Start with V1_MULTI ruleset (using no-overwrite config from the start)
        engine1 = new AstRulesEngine();
        engine1.initializeHA(HA_UUID, "worker-1", dbParamsJson, dbHAConfigJsonWithNoOverwrite);
        sessionId1 = engine1.createRuleset(RULE_SET_V1_MULTI, RuleConfigurationOption.FULLY_MANUAL_PSEUDOCLOCK);

        consumer1 = new HAIntegrationTestBase.AsyncConsumer("consumer1");
        consumer1.startConsuming(engine1.port());

        engine1.enableLeader();

        // Send only a temperature event — partial match (waiting for humidity)
        String tempEvent = createEvent("{\"temperature\": 35}");
        String result1 = engine1.assertEvent(sessionId1, tempEvent);
        List<Map<String, Object>> matchList1 = JsonMapper.readValueAsListOfMapOfStringAndObject(result1);
        assertThat(matchList1).isEmpty();

        // Verify partial event is persisted
        HAStateManager assertionManager = createHAStateManagerForAssertion();
        try {
            SessionState stateV1 = assertionManager.getPersistedSessionState("Test Ruleset");
            assertThat(stateV1).isNotNull();
            assertThat(stateV1.getPartialEvents())
                    .as("V1 should have a partial event (temperature waiting for humidity)")
                    .isNotEmpty();
            String v1Hash = stateV1.getRulebookHash();

            // Phase 2: Dispose and recreate with V2_MULTI ruleset (overwrite_if_rulebook_changes=false)
            engine1.dispose(sessionId1);
            sessionId1 = engine1.createRuleset(RULE_SET_V2_MULTI, RuleConfigurationOption.FULLY_MANUAL_PSEUDOCLOCK);

            // Verify the session state was NOT deleted (overwrite_if_rulebook_changes=false skips deletion)
            SessionState stateAfterUpdate = assertionManager.getPersistedSessionState("Test Ruleset");
            assertThat(stateAfterUpdate).isNotNull();
            assertThat(stateAfterUpdate.getRulebookHash()).isEqualTo(v1Hash);
            assertThat(stateAfterUpdate.getPartialEvents())
                    .as("Partial events should be preserved (overwrite_if_rulebook_changes=false)")
                    .isNotEmpty();

            // Phase 3: Sending pressure should fire V2_MULTI (recovered temperature + new pressure)
            String pressureEvent = createEvent("{\"pressure\": 150}");
            String result2 = engine1.assertEvent(sessionId1, pressureEvent);
            assertThat(result2)
                    .as("Rule should fire because temperature partial was recovered and pressure completes the match")
                    .contains("temp_and_pressure_alert");
        } finally {
            assertionManager.shutdown();
        }
    }

    /**
     * Scenario: Node2 has updated ruleset with overwrite_if_rulebook_changes=false and takes over as leader.
     *
     * 1. Node1 starts with V1_MULTI rules, becomes leader, sends temperature event (partial match)
     * 2. Node2 starts with V2_MULTI rules (overwrite_if_rulebook_changes=false)
     * 3. Node1 fails, Node2 becomes leader
     * 4. Node2 should recover partial events from V1 despite hash mismatch
     * 5. Sending pressure event should fire V2_MULTI rule (recovered temperature + new pressure)
     */
    @Test
    void testUpdateRulesetOnNode2WithNoOverwriteAndFailover() {
        // Phase 1: Node1 with V1_MULTI ruleset as leader
        engine1 = new AstRulesEngine();
        engine1.initializeHA(HA_UUID, "worker-1", dbParamsJson, dbHAConfigJson);
        sessionId1 = engine1.createRuleset(RULE_SET_V1_MULTI, RuleConfigurationOption.FULLY_MANUAL_PSEUDOCLOCK);

        consumer1 = new HAIntegrationTestBase.AsyncConsumer("consumer1");
        consumer1.startConsuming(engine1.port());

        engine1.enableLeader();

        // Send only a temperature event — partial match (waiting for humidity)
        String tempEvent = createEvent("{\"temperature\": 35}");
        String result1 = engine1.assertEvent(sessionId1, tempEvent);
        List<Map<String, Object>> matchList1 = JsonMapper.readValueAsListOfMapOfStringAndObject(result1);
        assertThat(matchList1).isEmpty();

        // Phase 2: Node2 starts with V2_MULTI ruleset (overwrite_if_rulebook_changes=false)
        engine2 = new AstRulesEngine();
        engine2.initializeHA(HA_UUID, "worker-2", dbParamsJson, dbHAConfigJsonWithNoOverwrite);
        sessionId2 = engine2.createRuleset(RULE_SET_V2_MULTI, RuleConfigurationOption.FULLY_MANUAL_PSEUDOCLOCK);

        consumer2 = new HAIntegrationTestBase.AsyncConsumer("consumer2");
        consumer2.startConsuming(engine2.port());

        // Phase 3: Failover - Node1 goes down, Node2 becomes leader
        engine1.disableLeader();
        engine2.enableLeader();

        // Phase 4: Sending pressure should fire V2_MULTI (recovered temperature + new pressure)
        String pressureEvent = createEvent("{\"pressure\": 150}");
        String result2 = engine2.assertEvent(sessionId2, pressureEvent);
        assertThat(result2)
                .as("Rule should fire because temperature partial was recovered and pressure completes the match")
                .contains("temp_and_pressure_alert");
    }

    /**
     * Scenario: Single server restarts with updated ruleset (overwrite_if_rulebook_changes=false).
     *
     * 1. Node1 starts with V1_MULTI rules, becomes leader, sends temperature event (partial match)
     * 2. Node1 shuts down (engine.close())
     * 3. Node1 restarts with V2_MULTI rules (overwrite_if_rulebook_changes=false), becomes leader
     * 4. Should recover partial events from V1 despite hash mismatch
     * 5. Sending pressure event should fire V2_MULTI rule (recovered temperature + new pressure)
     */
    @Test
    void testUpdateRulesetWithNoOverwriteAndRestart() {
        // Phase 1: Start with V1_MULTI ruleset
        engine1 = new AstRulesEngine();
        engine1.initializeHA(HA_UUID, "worker-1", dbParamsJson, dbHAConfigJson);
        sessionId1 = engine1.createRuleset(RULE_SET_V1_MULTI, RuleConfigurationOption.FULLY_MANUAL_PSEUDOCLOCK);

        consumer1 = new HAIntegrationTestBase.AsyncConsumer("consumer1");
        consumer1.startConsuming(engine1.port());

        engine1.enableLeader();

        // Send only a temperature event — partial match (waiting for humidity)
        String tempEvent = createEvent("{\"temperature\": 35}");
        String result1 = engine1.assertEvent(sessionId1, tempEvent);
        List<Map<String, Object>> matchList1 = JsonMapper.readValueAsListOfMapOfStringAndObject(result1);
        assertThat(matchList1).isEmpty();

        // Verify partial event is persisted
        HAStateManager assertionManager = createHAStateManagerForAssertion();
        try {
            SessionState stateV1 = assertionManager.getPersistedSessionState("Test Ruleset");
            assertThat(stateV1).isNotNull();
            assertThat(stateV1.getPartialEvents())
                    .as("V1 should have a partial event (temperature waiting for humidity)")
                    .isNotEmpty();
        } finally {
            assertionManager.shutdown();
        }

        // Phase 2: Shut down engine1 completely (simulates server restart)
        consumer1.stop();
        consumer1 = null;
        engine1.close();
        engine1 = null;

        // Phase 3: Restart with V2_MULTI ruleset (overwrite_if_rulebook_changes=false)
        engine1 = new AstRulesEngine();
        engine1.initializeHA(HA_UUID, "worker-1", dbParamsJson, dbHAConfigJsonWithNoOverwrite);
        sessionId1 = engine1.createRuleset(RULE_SET_V2_MULTI, RuleConfigurationOption.FULLY_MANUAL_PSEUDOCLOCK);

        consumer1 = new HAIntegrationTestBase.AsyncConsumer("consumer1-restarted");
        consumer1.startConsuming(engine1.port());

        engine1.enableLeader();

        // Phase 4: Sending pressure should fire V2_MULTI (recovered temperature + new pressure)
        String pressureEvent = createEvent("{\"pressure\": 150}");
        String result2 = engine1.assertEvent(sessionId1, pressureEvent);
        assertThat(result2)
                .as("Rule should fire because temperature partial was recovered and pressure completes the match")
                .contains("temp_and_pressure_alert");
    }

    /**
     * Scenario: Single server restarts with updated ruleset (overwrite_if_rulebook_changes=true, default).
     *
     * 1. Node1 starts with V1_MULTI rules, becomes leader, sends temperature event (partial match)
     * 2. Node1 shuts down (engine.close())
     * 3. Node1 restarts with V2_MULTI rules (overwrite=true, default), becomes leader
     * 4. Should detect hash mismatch, delete old state, and NOT recover partial events
     * 5. Sending only pressure event should NOT fire (temperature partial was not recovered)
     */
    @Test
    void testUpdateRulesetWithOverwriteAndRestart() {
        // Phase 1: Start with V1_MULTI ruleset
        engine1 = new AstRulesEngine();
        engine1.initializeHA(HA_UUID, "worker-1", dbParamsJson, dbHAConfigJson);
        sessionId1 = engine1.createRuleset(RULE_SET_V1_MULTI, RuleConfigurationOption.FULLY_MANUAL_PSEUDOCLOCK);

        consumer1 = new HAIntegrationTestBase.AsyncConsumer("consumer1");
        consumer1.startConsuming(engine1.port());

        engine1.enableLeader();

        // Send only a temperature event — partial match (waiting for humidity)
        String tempEvent = createEvent("{\"temperature\": 35}");
        String result1 = engine1.assertEvent(sessionId1, tempEvent);
        List<Map<String, Object>> matchList1 = JsonMapper.readValueAsListOfMapOfStringAndObject(result1);
        assertThat(matchList1).isEmpty();

        // Phase 2: Shut down engine1 completely (simulates server restart)
        consumer1.stop();
        consumer1 = null;
        engine1.close();
        engine1 = null;

        // Phase 3: Restart with V2_MULTI ruleset (overwrite=true, default)
        engine1 = new AstRulesEngine();
        engine1.initializeHA(HA_UUID, "worker-1", dbParamsJson, dbHAConfigJson);
        sessionId1 = engine1.createRuleset(RULE_SET_V2_MULTI, RuleConfigurationOption.FULLY_MANUAL_PSEUDOCLOCK);

        consumer1 = new HAIntegrationTestBase.AsyncConsumer("consumer1-restarted");
        consumer1.startConsuming(engine1.port());

        engine1.enableLeader();

        // Phase 4: Verify old partial events were deleted
        HAStateManager assertionManager = createHAStateManagerForAssertion();
        try {
            SessionState state = assertionManager.getPersistedSessionState("Test Ruleset");
            assertThat(state).isNotNull();
            String v1Hash = org.drools.ansible.rulebook.integration.ha.api.HAUtils.sha256(RULE_SET_V1_MULTI);
            String v2Hash = org.drools.ansible.rulebook.integration.ha.api.HAUtils.sha256(RULE_SET_V2_MULTI);
            assertThat(state.getRulebookHash()).isNotEqualTo(v1Hash);
            assertThat(state.getRulebookHash()).isEqualTo(v2Hash);
            assertThat(state.getPartialEvents())
                    .as("Old partial events should be deleted on overwrite")
                    .isEmpty();
        } finally {
            assertionManager.shutdown();
        }

        // Phase 5: Sending only pressure should NOT fire (temperature partial was not recovered)
        String pressureEvent = createEvent("{\"pressure\": 150}");
        String result2 = engine1.assertEvent(sessionId1, pressureEvent);
        List<Map<String, Object>> matchList2 = JsonMapper.readValueAsListOfMapOfStringAndObject(result2);
        assertThat(matchList2)
                .as("Rule should NOT fire because temperature partial event was not recovered")
                .isEmpty();
    }

    /**
     * Verifies that stale partial events from an old ruleset are cleared after recovery
     * when overwrite_if_rulebook_changes=false.
     *
     * 1. Node starts with V1_MULTI rules (AllCondition: temperature > 30 AND humidity > 50).
     *    Sends only a temperature event → partial match, event persisted in partialEvents.
     * 2. Node shuts down. Persisted state contains the stale temperature partial event.
     * 3. Node restarts with V_PRESSURE rules (pressure > 100) — completely different domain.
     *    Because overwrite_if_rulebook_changes=false, the old state is recovered.
     *    During recovery, Drools replays the temperature event but discards it (no matching rule).
     *    After recovery, the refreshed state is persisted — the stale partial event is gone.
     */
    @Test
    void testStalePartialEventsClearedAfterRecoveryWithNoOverwrite() {
        // Phase 1: Start with V1_MULTI ruleset (requires both temperature AND humidity)
        engine1 = new AstRulesEngine();
        engine1.initializeHA(HA_UUID, "worker-1", dbParamsJson, dbHAConfigJsonWithNoOverwrite);
        sessionId1 = engine1.createRuleset(RULE_SET_V1_MULTI, RuleConfigurationOption.FULLY_MANUAL_PSEUDOCLOCK);

        consumer1 = new HAIntegrationTestBase.AsyncConsumer("consumer1");
        consumer1.startConsuming(engine1.port());

        engine1.enableLeader();

        // Send only a temperature event — this creates a partial match (waiting for humidity)
        String tempEvent = createEvent("{\"temperature\": 35}");
        String result1 = engine1.assertEvent(sessionId1, tempEvent);
        // No match yet — rule needs both temperature AND humidity
        List<Map<String, Object>> matchList1 = JsonMapper.readValueAsListOfMapOfStringAndObject(result1);
        assertThat(matchList1).isEmpty();

        // Verify partial event is persisted
        HAStateManager assertionManager = createHAStateManagerForAssertion();
        try {
            SessionState stateV1 = assertionManager.getPersistedSessionState("Test Ruleset");
            assertThat(stateV1).isNotNull();
            assertThat(stateV1.getPartialEvents())
                    .as("V1 should have a partial event (temperature waiting for humidity)")
                    .isNotEmpty();
        } finally {
            assertionManager.shutdown();
        }

        // Phase 2: Shut down and restart with completely different ruleset (pressure rules)
        consumer1.stop();
        consumer1 = null;
        engine1.close();
        engine1 = null;

        engine1 = new AstRulesEngine();
        engine1.initializeHA(HA_UUID, "worker-1", dbParamsJson, dbHAConfigJsonWithNoOverwrite);
        sessionId1 = engine1.createRuleset(RULE_SET_V_PRESSURE, RuleConfigurationOption.FULLY_MANUAL_PSEUDOCLOCK);

        consumer1 = new HAIntegrationTestBase.AsyncConsumer("consumer1-pressure");
        consumer1.startConsuming(engine1.port());

        engine1.enableLeader();

        // Phase 3: Verify the stale temperature partial event has been cleared after recovery
        assertionManager = createHAStateManagerForAssertion();
        try {
            SessionState stateAfterRestart = assertionManager.getPersistedSessionState("Test Ruleset");
            assertThat(stateAfterRestart).isNotNull();

            boolean hasTemperatureEvent = stateAfterRestart.getPartialEvents().stream()
                    .anyMatch(e -> e.getEventJson().contains("temperature"));
            assertThat(hasTemperatureEvent)
                    .as("Stale temperature partial event from V1_MULTI should be cleared " +
                        "after recovery persists the refreshed state")
                    .isFalse();
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
