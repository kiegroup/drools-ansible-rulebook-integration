package org.drools.ansible.rulebook.integration.ha.tests.integration.temporal;

import org.drools.ansible.rulebook.integration.ha.tests.support.TestUtils;

import org.drools.ansible.rulebook.integration.ha.tests.support.TestOutputCapture;

import org.drools.ansible.rulebook.integration.ha.tests.support.AbstractHATestBase;

import org.drools.ansible.rulebook.integration.ha.tests.integration.HAIntegrationTestBase;

import java.util.List;
import java.util.Map;

import org.drools.ansible.rulebook.integration.api.RuleConfigurationOption;
import org.drools.ansible.rulebook.integration.core.jpy.AstRulesEngine;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManager;
import org.drools.ansible.rulebook.integration.ha.model.MatchingEvent;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.readValueAsListOfMapOfStringAndObject;
import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.toJson;
import static org.drools.ansible.rulebook.integration.ha.tests.support.TestUtils.createEvent;

/**
 * Tests for the expired_window_grace_period configuration option with not_all+timeout rules.
 *
 * When a crash happens before the timeout expires and the timeout expires during the outage,
 * the grace period controls whether the match is captured and dispatched or dropped.
 */
class HAIntegrationTimedOutGracePeriodTest extends AbstractHATestBase {

    private static final String HA_UUID = "grace-timedout-test-ha-1";

    private static final String RULE_SET_TIMED_OUT = """
                {
                    "name": "GracePeriod TimedOut Ruleset",
                    "rules": [
                        {"Rule": {
                            "name": "maint failed",
                            "condition": {
                                "NotAllCondition": [
                                    {
                                        "EqualsExpression": {
                                            "lhs": {
                                                "Event": "alert.code"
                                            },
                                            "rhs": {
                                                "Integer": 1001
                                            }
                                        }
                                    },
                                    {
                                        "EqualsExpression": {
                                            "lhs": {
                                                "Event": "alert.code"
                                            },
                                            "rhs": {
                                                "Integer": 1002
                                            }
                                        }
                                    }
                                ],
                                "timeout": "10 seconds"
                            },
                            "action": {
                                "Action": {
                                    "action": "print_event",
                                    "action_args": {}
                                }
                            },
                            "enabled": true
                        }}
                    ]
                }
                """;

    static {
        if (USE_POSTGRES) {
            initializePostgres("eda_ha_grace_timedout_test", "Grace period timed out tests");
        } else {
            initializeH2();
        }
    }

    protected AstRulesEngine rulesEngine1;
    protected AstRulesEngine rulesEngine2;

    protected long sessionId1;
    protected long sessionId2;

    protected HAIntegrationTestBase.AsyncConsumer consumer1;
    protected HAIntegrationTestBase.AsyncConsumer consumer2;

    private void setUpWithGracePeriod(int gracePeriodSeconds) {
        Map<String, Object> haConfig = Map.of(
                "write_after", 1,
                "expired_window_grace_period", gracePeriodSeconds
        );
        String haConfigJson = toJson(haConfig);

        rulesEngine1 = new AstRulesEngine();
        consumer1 = new HAIntegrationTestBase.AsyncConsumer("consumer1");
        consumer1.startConsuming(rulesEngine1.port());
        rulesEngine1.initializeHA(HA_UUID, "worker-1", dbParamsJson, haConfigJson);
        sessionId1 = rulesEngine1.createRuleset(RULE_SET_TIMED_OUT, RuleConfigurationOption.FULLY_MANUAL_PSEUDOCLOCK);

        rulesEngine2 = new AstRulesEngine();
        consumer2 = new HAIntegrationTestBase.AsyncConsumer("consumer2");
        consumer2.startConsuming(rulesEngine2.port());
        rulesEngine2.initializeHA(HA_UUID, "worker-2", dbParamsJson, haConfigJson);
        sessionId2 = rulesEngine2.createRuleset(RULE_SET_TIMED_OUT, RuleConfigurationOption.FULLY_MANUAL_PSEUDOCLOCK);
    }

    private void tearDownEngines() {
        if (consumer1 != null) consumer1.stop();
        if (consumer2 != null) consumer2.stop();
        if (rulesEngine1 != null) {
            rulesEngine1.dispose(sessionId1);
            rulesEngine1.close();
        }
        if (rulesEngine2 != null) {
            rulesEngine2.dispose(sessionId2);
            rulesEngine2.close();
        }
        cleanupDatabase();
    }

    /**
     * Partial match (code=1001) at T=0, timeout=10s, crash at T=8, Node2 clock at T=15 (5s past timeout).
     * Grace=600s. The match should be captured and dispatched as a MatchingEvent.
     */
    @Test
    void testTimedOutWithinGracePeriod() {
        setUpWithGracePeriod(600);
        try {
            rulesEngine1.enableLeader();

            // Send partial match: code=1001 (only 1 of 2 conditions met)
            String event = createEvent("{\"alert\":{\"code\":1001,\"message\":\"Applying maintenance\"}}");
            assertThat(readValueAsListOfMapOfStringAndObject(rulesEngine1.assertEvent(sessionId1, event))).isEmpty();

            // Advance Node1 to T=8s (timeout at T=10s)
            rulesEngine1.advanceTime(sessionId1, 8, "SECONDS");

            // Advance Node2 (follower) to T=15s so recovery clock jump triggers timeout
            rulesEngine2.advanceTime(sessionId2, 15, "SECONDS");

            // Simulate failover
            rulesEngine1.disableLeader();
            rulesEngine1.close();
            rulesEngine1 = null;
            consumer1.stop();
            consumer1 = null;

            // Node2 becomes leader — recovery advances clock from T=8s (persisted) to T=15s (Node2's clock)
            // The timeout expires during this clock jump (5s past timeout, within 600s grace)
            rulesEngine2.enableLeader();

            // Give async channel a moment to deliver
            Thread.sleep(500);

            List<String> messages = consumer2.getReceivedMessages();
            assertThat(messages)
                    .as("Grace period match should be dispatched via async channel as MATCHING_EVENT_RECOVERY")
                    .isNotEmpty();

            // Verify the match is in the database
            HAStateManager assertionManager = createHAStateManagerForAssertion();
            List<MatchingEvent> pendingEvents = assertionManager.getPendingMatchingEvents();
            assertThat(pendingEvents)
                    .as("Grace period recovery match should be persisted in database")
                    .isNotEmpty();

            // Verify match details
            MatchingEvent me = pendingEvents.stream()
                    .filter(e -> "maint failed".equals(e.getRuleName()))
                    .findFirst()
                    .orElse(null);
            assertThat(me).isNotNull();
            assertThat(me.getRuleSetName()).isEqualTo("GracePeriod TimedOut Ruleset");

            // After recovery, advancing time should NOT produce a duplicate match
            String result = rulesEngine2.advanceTime(sessionId2, 5, "SECONDS");
            List<Map<String, Object>> matches = readValueAsListOfMapOfStringAndObject(result);
            assertThat(matches)
                    .as("No duplicate match should fire after grace period recovery")
                    .isEmpty();

            assertionManager.shutdown();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            tearDownEngines();
        }
    }

    /**
     * Partial match (code=1001) at T=0, timeout=10s, crash at T=8, Node2 clock at T=700 (690s past timeout).
     * Grace=600s. The match should be dropped (outside grace period).
     */
    @Test
    void testTimedOutOutsideGracePeriod() {
        setUpWithGracePeriod(600);
        try {
            rulesEngine1.enableLeader();

            // Send partial match: code=1001
            String event = createEvent("{\"alert\":{\"code\":1001,\"message\":\"Applying maintenance\"}}");
            assertThat(readValueAsListOfMapOfStringAndObject(rulesEngine1.assertEvent(sessionId1, event))).isEmpty();

            // Advance Node1 to T=8s
            rulesEngine1.advanceTime(sessionId1, 8, "SECONDS");

            // Advance Node2 to T=700s (690s past the 10s timeout)
            rulesEngine2.advanceTime(sessionId2, 700, "SECONDS");

            // Simulate failover
            rulesEngine1.disableLeader();
            rulesEngine1.close();
            rulesEngine1 = null;
            consumer1.stop();
            consumer1 = null;

            // Node2 becomes leader — the timeout expired 690s ago, outside 600s grace period
            String logs;
            try {
                logs = TestOutputCapture.captureStdout(() -> rulesEngine2.enableLeader());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            assertThat(logs).contains("Dropping expired recovery match for rule 'maint failed'");

            // The match should NOT be persisted
            HAStateManager assertionManager = createHAStateManagerForAssertion();
            List<MatchingEvent> pendingEvents = assertionManager.getPendingMatchingEvents();
            boolean hasMaintFailed = pendingEvents.stream()
                    .anyMatch(e -> "maint failed".equals(e.getRuleName()));
            assertThat(hasMaintFailed)
                    .as("Match outside grace period should be dropped, not persisted")
                    .isFalse();

            // Advancing time should produce no match (already fired and dropped during recovery)
            String result = rulesEngine2.advanceTime(sessionId2, 5, "SECONDS");
            List<Map<String, Object>> matches = readValueAsListOfMapOfStringAndObject(result);
            assertThat(matches).isEmpty();

            assertionManager.shutdown();
        } finally {
            tearDownEngines();
        }
    }

    /**
     * Grace period = 0 (default). The match fires during recovery but is not dispatched.
     * This verifies the current default behavior is preserved.
     */
    @Test
    void testGracePeriodZeroPreservesDefaultBehavior() {
        setUpWithGracePeriod(0);
        try {
            rulesEngine1.enableLeader();

            // Send partial match: code=1001
            String event = createEvent("{\"alert\":{\"code\":1001,\"message\":\"Applying maintenance\"}}");
            assertThat(readValueAsListOfMapOfStringAndObject(rulesEngine1.assertEvent(sessionId1, event))).isEmpty();

            // Advance Node1 to T=8s
            rulesEngine1.advanceTime(sessionId1, 8, "SECONDS");

            // Advance Node2 past timeout
            rulesEngine2.advanceTime(sessionId2, 15, "SECONDS");

            // Simulate failover
            rulesEngine1.disableLeader();
            rulesEngine1.close();
            rulesEngine1 = null;
            consumer1.stop();
            consumer1 = null;

            // Node2 becomes leader — timeout expires during recovery but grace=0 means no dispatch
            rulesEngine2.enableLeader();

            // No MatchingEvent should be persisted
            HAStateManager assertionManager = createHAStateManagerForAssertion();
            List<MatchingEvent> pendingEvents = assertionManager.getPendingMatchingEvents();
            boolean hasMaintFailed = pendingEvents.stream()
                    .anyMatch(e -> "maint failed".equals(e.getRuleName()));
            assertThat(hasMaintFailed)
                    .as("Grace period=0 should not persist recovery matches")
                    .isFalse();

            // No match from subsequent advanceTime either
            String result = rulesEngine2.advanceTime(sessionId2, 5, "SECONDS");
            List<Map<String, Object>> matches = readValueAsListOfMapOfStringAndObject(result);
            assertThat(matches).isEmpty();

            assertionManager.shutdown();
        } finally {
            tearDownEngines();
        }
    }

    /**
     * All conditions met after recovery — rule should NOT fire even with grace period.
     * Partial match (code=1001) on Node1, failover, Node2 sends code=1002 within timeout.
     */
    @Test
    void testAllConditionsMetAfterRecoveryWithGracePeriod() {
        setUpWithGracePeriod(600);
        try {
            rulesEngine1.enableLeader();

            // Send partial match: code=1001
            String event1 = createEvent("{\"alert\":{\"code\":1001,\"message\":\"Applying maintenance\"}}");
            assertThat(readValueAsListOfMapOfStringAndObject(rulesEngine1.assertEvent(sessionId1, event1))).isEmpty();

            // Advance Node1 to T=3s
            rulesEngine1.advanceTime(sessionId1, 3, "SECONDS");
            rulesEngine2.advanceTime(sessionId2, 3, "SECONDS");

            // Simulate failover
            rulesEngine1.disableLeader();
            rulesEngine1.close();
            rulesEngine1 = null;
            consumer1.stop();
            consumer1 = null;

            rulesEngine2.enableLeader();

            // Send the second condition (code=1002) on Node2 within the timeout
            rulesEngine2.advanceTime(sessionId2, 1, "SECONDS");
            String event2 = createEvent("{\"alert\":{\"code\":1002,\"message\":\"Maintenance completed\"}}");
            assertThat(readValueAsListOfMapOfStringAndObject(rulesEngine2.assertEvent(sessionId2, event2))).isEmpty();

            // Advance past the timeout — rule should NOT fire because all conditions were met
            String result = rulesEngine2.advanceTime(sessionId2, 7, "SECONDS");
            List<Map<String, Object>> matches = readValueAsListOfMapOfStringAndObject(result);
            assertThat(matches)
                    .as("All conditions met — timed_out rule should not fire even with grace period")
                    .isEmpty();

            // No MatchingEvent should be persisted
            HAStateManager assertionManager = createHAStateManagerForAssertion();
            List<MatchingEvent> pendingEvents = assertionManager.getPendingMatchingEvents();
            boolean hasMaintFailed = pendingEvents.stream()
                    .anyMatch(e -> "maint failed".equals(e.getRuleName()));
            assertThat(hasMaintFailed)
                    .as("All conditions met — no recovery match should be persisted")
                    .isFalse();

            assertionManager.shutdown();
        } finally {
            tearDownEngines();
        }
    }

    private HAStateManager createHAStateManagerForAssertion() {
        HAStateManager manager = org.drools.ansible.rulebook.integration.ha.api.HAStateManagerFactory.create(TEST_DB_TYPE);
        manager.initializeHA(HA_UUID, "FOR_ASSERTION", dbParams, dbHAConfig);
        return manager;
    }
}
