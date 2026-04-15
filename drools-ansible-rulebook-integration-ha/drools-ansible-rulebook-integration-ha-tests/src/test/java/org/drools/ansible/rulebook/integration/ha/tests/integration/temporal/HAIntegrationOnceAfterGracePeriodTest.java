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
import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.readValueAsMapOfStringAndObject;
import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.toJson;
import static org.drools.ansible.rulebook.integration.ha.tests.support.TestUtils.createEvent;

/**
 * Tests for the expired_window_grace_period configuration option with once_after rules.
 *
 * When a crash happens before a time-window expires and the window expires during the outage,
 * the grace period controls whether the match is captured and dispatched or dropped.
 */
class HAIntegrationOnceAfterGracePeriodTest extends AbstractHATestBase {

    private static final String HA_UUID = "grace-period-test-ha-1";

    private static final String RULE_SET_ONCE_AFTER = """
                {
                    "name": "GracePeriod OnceAfter Ruleset",
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
                                    "once_after": "10 seconds"
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

    static {
        if (USE_POSTGRES) {
            initializePostgres("eda_ha_grace_test", "Grace period tests");
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
        sessionId1 = rulesEngine1.createRuleset(RULE_SET_ONCE_AFTER, RuleConfigurationOption.FULLY_MANUAL_PSEUDOCLOCK);

        rulesEngine2 = new AstRulesEngine();
        consumer2 = new HAIntegrationTestBase.AsyncConsumer("consumer2");
        consumer2.startConsuming(rulesEngine2.port());
        rulesEngine2.initializeHA(HA_UUID, "worker-2", dbParamsJson, haConfigJson);
        sessionId2 = rulesEngine2.createRuleset(RULE_SET_ONCE_AFTER, RuleConfigurationOption.FULLY_MANUAL_PSEUDOCLOCK);
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
     * Event at T=0, window=10s, crash at T=8, Node2 clock at T=15 (5s past expiry).
     * Grace=600s. The match should be captured and dispatched as a MatchingEvent.
     */
    @Test
    void testOnceAfterWithinGracePeriod() {
        setUpWithGracePeriod(600);
        try {
            rulesEngine1.enableLeader();

            // Send event at T=0
            String event = createEvent("{\"alert\":{\"type\":\"warning\",\"host\":\"h1\"}}");
            assertThat(readValueAsListOfMapOfStringAndObject(rulesEngine1.assertEvent(sessionId1, event))).isEmpty();

            // Advance Node1 to T=8s (timer set to expire at T=10s)
            rulesEngine1.advanceTime(sessionId1, 8, "SECONDS");

            // Advance Node2 (follower) to T=15s so recovery clock jump triggers expiration
            rulesEngine2.advanceTime(sessionId2, 15, "SECONDS");

            // Simulate failover
            rulesEngine1.disableLeader();
            rulesEngine1.close();
            rulesEngine1 = null;
            consumer1.stop();
            consumer1 = null;

            // Node2 becomes leader — recovery advances clock from T=8s (persisted) to T=15s (Node2's clock)
            // The once_after timer expires during this clock jump (5s past expiry, within 600s grace)
            rulesEngine2.enableLeader();

            // The match should have been persisted as a MatchingEvent during recovery.
            // Verify by checking that the async consumer received a recovery message.
            // Give async channel a moment to deliver
            Thread.sleep(500);

            List<String> messages = consumer2.getReceivedMessages();
            assertThat(messages)
                    .as("Grace period match should be dispatched via async channel as MATCHING_EVENT_RECOVERY")
                    .isNotEmpty();

            // Also verify the match is in the database
            HAStateManager assertionManager = createHAStateManagerForAssertion();
            List<MatchingEvent> pendingEvents = assertionManager.getPendingMatchingEvents();
            assertThat(pendingEvents)
                    .as("Grace period recovery match should be persisted in database")
                    .isNotEmpty();

            // Verify match details
            MatchingEvent me = pendingEvents.stream()
                    .filter(e -> "alert_throttle".equals(e.getRuleName()))
                    .findFirst()
                    .orElse(null);
            assertThat(me).isNotNull();
            assertThat(me.getRuleSetName()).isEqualTo("GracePeriod OnceAfter Ruleset");

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
     * Event at T=0, window=10s, crash at T=8, Node2 clock at T=700 (690s past expiry).
     * Grace=600s. The match should be dropped (outside grace period).
     */
    @Test
    void testOnceAfterOutsideGracePeriod() {
        setUpWithGracePeriod(600);
        try {
            rulesEngine1.enableLeader();

            // Send event at T=0
            String event = createEvent("{\"alert\":{\"type\":\"warning\",\"host\":\"h1\"}}");
            assertThat(readValueAsListOfMapOfStringAndObject(rulesEngine1.assertEvent(sessionId1, event))).isEmpty();

            // Advance Node1 to T=8s
            rulesEngine1.advanceTime(sessionId1, 8, "SECONDS");

            // Advance Node2 to T=700s (690s past the 10s window expiry)
            rulesEngine2.advanceTime(sessionId2, 700, "SECONDS");

            // Simulate failover
            rulesEngine1.disableLeader();
            rulesEngine1.close();
            rulesEngine1 = null;
            consumer1.stop();
            consumer1 = null;

            // Node2 becomes leader — the timer expired 690s ago, outside 600s grace period
            String logs;
            try {
                logs = TestOutputCapture.captureStdout(() -> rulesEngine2.enableLeader());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            assertThat(logs).contains("Dropping expired recovery match for rule 'alert_throttle'");

            // The match should NOT be persisted
            HAStateManager assertionManager = createHAStateManagerForAssertion();
            List<MatchingEvent> pendingEvents = assertionManager.getPendingMatchingEvents();
            boolean hasAlertThrottle = pendingEvents.stream()
                    .anyMatch(e -> "alert_throttle".equals(e.getRuleName()));
            assertThat(hasAlertThrottle)
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

            // Send event at T=0
            String event = createEvent("{\"alert\":{\"type\":\"warning\",\"host\":\"h1\"}}");
            assertThat(readValueAsListOfMapOfStringAndObject(rulesEngine1.assertEvent(sessionId1, event))).isEmpty();

            // Advance Node1 to T=8s
            rulesEngine1.advanceTime(sessionId1, 8, "SECONDS");

            // Advance Node2 past window expiry
            rulesEngine2.advanceTime(sessionId2, 15, "SECONDS");

            // Simulate failover
            rulesEngine1.disableLeader();
            rulesEngine1.close();
            rulesEngine1 = null;
            consumer1.stop();
            consumer1 = null;

            // Node2 becomes leader — timer expires during recovery but grace=0 means no dispatch
            rulesEngine2.enableLeader();

            // No MatchingEvent should be persisted
            HAStateManager assertionManager = createHAStateManagerForAssertion();
            List<MatchingEvent> pendingEvents = assertionManager.getPendingMatchingEvents();
            boolean hasAlertThrottle = pendingEvents.stream()
                    .anyMatch(e -> "alert_throttle".equals(e.getRuleName()));
            assertThat(hasAlertThrottle)
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
     * Multiple events within the once_after window, crash, recovery within grace period.
     * Verifies that the aggregated match (with correct events_in_window count) is dispatched.
     */
    @Test
    void testOnceAfterMultipleEventsWithinGracePeriod() {
        setUpWithGracePeriod(600);
        try {
            rulesEngine1.enableLeader();

            // Send 3 events at T=0, T=1, T=2
            for (int i = 0; i < 3; i++) {
                String event = createEvent("{\"alert\":{\"type\":\"warning\",\"host\":\"h1\"}}");
                assertThat(readValueAsListOfMapOfStringAndObject(rulesEngine1.assertEvent(sessionId1, event))).isEmpty();
                rulesEngine1.advanceTime(sessionId1, 1, "SECONDS");
            }

            // Node1 at T=3s, advance to T=8s
            rulesEngine1.advanceTime(sessionId1, 5, "SECONDS");

            // Node2 at T=15s
            rulesEngine2.advanceTime(sessionId2, 15, "SECONDS");

            // Simulate failover
            rulesEngine1.disableLeader();
            rulesEngine1.close();
            rulesEngine1 = null;
            consumer1.stop();
            consumer1 = null;

            rulesEngine2.enableLeader();

            // Verify match is persisted
            HAStateManager assertionManager = createHAStateManagerForAssertion();
            List<MatchingEvent> pendingEvents = assertionManager.getPendingMatchingEvents();
            MatchingEvent me = pendingEvents.stream()
                    .filter(e -> "alert_throttle".equals(e.getRuleName()))
                    .findFirst()
                    .orElse(null);
            assertThat(me)
                    .as("Multi-event grace period recovery match should be persisted")
                    .isNotNull();

            // Verify the match contains 3 aggregated events
            // Structure: {"m": {"alert": {...}, "meta": {"rule_engine": {"events_in_window": 3, ...}}}}
            Map<String, Object> eventData = readValueAsMapOfStringAndObject(me.getEventData());
            @SuppressWarnings("unchecked")
            Map<String, Object> m = (Map<String, Object>) eventData.get("m");
            @SuppressWarnings("unchecked")
            Map<String, Object> meta = (Map<String, Object>) m.get("meta");
            @SuppressWarnings("unchecked")
            Map<String, Object> ruleEngine = (Map<String, Object>) meta.get("rule_engine");
            assertThat(ruleEngine)
                    .as("Match should have events_in_window=3")
                    .containsEntry("events_in_window", 3);

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
