package org.drools.ansible.rulebook.integration.ha.tests.integration.temporal.realtime;

import org.drools.ansible.rulebook.integration.ha.tests.support.TestUtils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.drools.ansible.rulebook.integration.core.jpy.AstRulesEngine;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManager;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManagerFactory;
import org.drools.ansible.rulebook.integration.ha.model.MatchingEvent;
import org.drools.ansible.rulebook.integration.ha.tests.support.AbstractHATestBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.readValueAsMapOfStringAndObject;
import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.toJson;
import static org.drools.ansible.rulebook.integration.ha.tests.support.TestUtils.createEvent;

/**
 * Grace period tests with AutomaticPseudoClock (real-time).
 * Unlike HAIntegrationGracePeriodTest which uses FULLY_MANUAL_PSEUDOCLOCK,
 * these tests let the auto-clock run naturally so windows expire via
 * the daemon thread's scheduledAdvanceTimeToMills() path.
 *
 * Scenario: Insert event on Node1 leader, failover before the window expires,
 * Node2 recovers and the once_after window expires during recovery clock jump.
 * The grace period determines whether the match is dispatched.
 */
class HAIntegrationOnceAfterGracePeriodAutoClockTest extends AbstractHATestBase {

    private static final String HA_UUID = "grace-autoclock-ha-1";

    private static final String RULE_SET_ONCE_AFTER_3S = """
            {
                "name": "GracePeriod AutoClock Ruleset",
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
                                "once_after": "3 seconds"
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
            initializePostgres("eda_ha_grace_autoclock_test", "Grace period auto-clock tests");
        } else {
            initializeH2();
        }
    }

    private AstRulesEngine rulesEngine1;
    private AstRulesEngine rulesEngine2;
    private long sessionId1;
    private long sessionId2;
    private HAIntegrationOnceAfterAutoClockTest.ThreadSafeAsyncConsumer consumer1;
    private HAIntegrationOnceAfterAutoClockTest.ThreadSafeAsyncConsumer consumer2;

    private void setUpWithGracePeriod(int gracePeriodSeconds) {
        Map<String, Object> haConfig = Map.of(
                "write_after", 1,
                "expired_window_grace_period", gracePeriodSeconds
        );
        String haConfigJson = toJson(haConfig);

        rulesEngine1 = new AstRulesEngine();
        consumer1 = new HAIntegrationOnceAfterAutoClockTest.ThreadSafeAsyncConsumer("consumer1");
        consumer1.startConsuming(rulesEngine1.port());
        rulesEngine1.initializeHA(HA_UUID, "worker-1", dbParamsJson, haConfigJson);
        // No FULLY_MANUAL_PSEUDOCLOCK — auto-clock runs naturally
        sessionId1 = rulesEngine1.createRuleset(RULE_SET_ONCE_AFTER_3S);

        rulesEngine2 = new AstRulesEngine();
        consumer2 = new HAIntegrationOnceAfterAutoClockTest.ThreadSafeAsyncConsumer("consumer2");
        consumer2.startConsuming(rulesEngine2.port());
        rulesEngine2.initializeHA(HA_UUID, "worker-2", dbParamsJson, haConfigJson);
        sessionId2 = rulesEngine2.createRuleset(RULE_SET_ONCE_AFTER_3S);
    }

    @AfterEach
    void tearDown() {
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
     * Insert event on Node1 leader, failover before window expires.
     * Wait for the 3s window to expire in real time on Node2, then enable leader.
     * The window expired only a few seconds ago — well within grace period.
     * The match should be dispatched as MATCHING_EVENT_RECOVERY.
     */
    @Test
    void testFailoverAndRecoveryWithinGracePeriod() {
        setUpWithGracePeriod(600);

        rulesEngine1.enableLeader();

        // Insert event on Node1
        String event = createEvent("{\"alert\":{\"type\":\"warning\",\"host\":\"h1\"}}");
        rulesEngine1.assertEvent(sessionId1, event);

        // Failover immediately (before the 3s window expires)
        rulesEngine1.disableLeader();
        rulesEngine1.dispose(sessionId1);
        rulesEngine1.close();
        rulesEngine1 = null;
        consumer1.stop();
        consumer1 = null;

        // Wait for the once_after window to expire in real time (3s + margin)
        await().pollDelay(4, TimeUnit.SECONDS).atMost(6, TimeUnit.SECONDS).until(() -> true);

        // Node2 becomes leader — recovery clock jump spans the window expiry
        // The window expired only ~1-3s ago, well within 600s grace
        rulesEngine2.enableLeader();

        // Wait for MATCHING_EVENT_RECOVERY on consumer2
        await().atMost(10, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .until(() -> consumer2.getReceivedMessages().stream()
                        .anyMatch(msg -> {
                            Map<String, Object> resp = parseAsyncMessage(msg);
                            Map<String, Object> result = getResult(resp);
                            return result != null && "MATCHING_EVENT_RECOVERY".equals(result.get("type"));
                        }));

        // Verify the recovery message
        String recoveryMessage = consumer2.getReceivedMessages().stream()
                .filter(msg -> {
                    Map<String, Object> resp = parseAsyncMessage(msg);
                    Map<String, Object> result = getResult(resp);
                    return result != null && "MATCHING_EVENT_RECOVERY".equals(result.get("type"));
                })
                .findFirst()
                .orElseThrow();

        Map<String, Object> resp = parseAsyncMessage(recoveryMessage);
        Map<String, Object> result = getResult(resp);
        assertThat(result)
                .containsEntry("name", "alert_throttle")
                .containsEntry("type", "MATCHING_EVENT_RECOVERY")
                .containsKey("matching_uuid");

        // Verify MatchingEvent persisted in DB
        HAStateManager assertionManager = createHAStateManagerForAssertion();
        try {
            List<MatchingEvent> pendingEvents = assertionManager.getPendingMatchingEvents();
            assertThat(pendingEvents)
                    .extracting(MatchingEvent::getRuleName)
                    .contains("alert_throttle");
        } finally {
            assertionManager.shutdown();
        }
    }

    /**
     * Grace period = 0 (default). Failover, window expires in real time, recovery.
     * The match fires during recovery but is silently dropped (no dispatch).
     */
    @Test
    void testFailoverAndRecoveryWithGracePeriodZero() {
        setUpWithGracePeriod(0);

        rulesEngine1.enableLeader();

        // Insert event on Node1
        String event = createEvent("{\"alert\":{\"type\":\"warning\",\"host\":\"h1\"}}");
        rulesEngine1.assertEvent(sessionId1, event);

        // Failover immediately
        rulesEngine1.disableLeader();
        rulesEngine1.dispose(sessionId1);
        rulesEngine1.close();
        rulesEngine1 = null;
        consumer1.stop();
        consumer1 = null;

        // Wait for the once_after window to expire in real time
        await().pollDelay(4, TimeUnit.SECONDS).atMost(6, TimeUnit.SECONDS).until(() -> true);

        // Node2 becomes leader — grace=0 means no dispatch
        rulesEngine2.enableLeader();

        // Give some time for any potential async messages
        await().pollDelay(2, TimeUnit.SECONDS).atMost(4, TimeUnit.SECONDS).until(() -> true);

        // No MATCHING_EVENT_RECOVERY should be received
        boolean hasRecoveryMessage = consumer2.getReceivedMessages().stream()
                .anyMatch(msg -> {
                    Map<String, Object> resp = parseAsyncMessage(msg);
                    Map<String, Object> result = getResult(resp);
                    return result != null && "MATCHING_EVENT_RECOVERY".equals(result.get("type"));
                });
        assertThat(hasRecoveryMessage)
                .as("Grace period=0 should not dispatch recovery matches")
                .isFalse();

        // No MatchingEvent in DB
        HAStateManager assertionManager = createHAStateManagerForAssertion();
        try {
            List<MatchingEvent> pendingEvents = assertionManager.getPendingMatchingEvents();
            boolean hasAlertThrottle = pendingEvents.stream()
                    .anyMatch(e -> "alert_throttle".equals(e.getRuleName()));
            assertThat(hasAlertThrottle)
                    .as("Grace period=0 should not persist recovery matches")
                    .isFalse();
        } finally {
            assertionManager.shutdown();
        }
    }

    /**
     * Insert event on Node1, wait for auto-clock to fire normally (no crash),
     * then failover. Node2 recovers the pending MatchingEvent via
     * recoverPendingMatchingEvents (not grace period). This verifies that
     * grace period config does not interfere with the normal auto-clock path.
     */
    @Test
    void testAutoClockFiresNormallyThenFailoverWithGracePeriod() {
        setUpWithGracePeriod(600);

        rulesEngine1.enableLeader();

        // Insert event on Node1
        String event = createEvent("{\"alert\":{\"type\":\"warning\",\"host\":\"h1\"}}");
        rulesEngine1.assertEvent(sessionId1, event);

        // Wait for auto-clock to fire the match on Node1 normally
        await().atMost(10, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .until(() -> !consumer1.getReceivedMessages().isEmpty());

        // Extract matching_uuid from Node1's normal match
        Map<String, Object> node1Resp = parseAsyncMessage(consumer1.getReceivedMessages().get(0));
        Map<String, Object> node1Result = getResult(node1Resp);
        assertThat(node1Result)
                .containsEntry("name", "alert_throttle")
                .containsKey("matching_uuid");
        String matchingUuid = (String) node1Result.get("matching_uuid");

        // Failover
        rulesEngine1.disableLeader();
        rulesEngine1.dispose(sessionId1);
        rulesEngine1.close();
        rulesEngine1 = null;
        consumer1.stop();
        consumer1 = null;

        // Node2 becomes leader — should recover the pending MatchingEvent
        rulesEngine2.enableLeader();

        // Wait for MATCHING_EVENT_RECOVERY on consumer2
        await().atMost(10, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .until(() -> consumer2.getReceivedMessages().stream()
                        .anyMatch(msg -> {
                            Map<String, Object> resp = parseAsyncMessage(msg);
                            Map<String, Object> result = getResult(resp);
                            return result != null && "MATCHING_EVENT_RECOVERY".equals(result.get("type"));
                        }));

        // Verify recovery message has the same matching_uuid
        String recoveryMessage = consumer2.getReceivedMessages().stream()
                .filter(msg -> {
                    Map<String, Object> resp = parseAsyncMessage(msg);
                    Map<String, Object> result = getResult(resp);
                    return result != null && "MATCHING_EVENT_RECOVERY".equals(result.get("type"));
                })
                .findFirst()
                .orElseThrow();

        Map<String, Object> recoveryResp = parseAsyncMessage(recoveryMessage);
        Map<String, Object> recoveryResult = getResult(recoveryResp);
        assertThat(recoveryResult)
                .containsEntry("matching_uuid", matchingUuid)
                .containsEntry("type", "MATCHING_EVENT_RECOVERY");
    }

    // === Helpers ===

    @SuppressWarnings("unchecked")
    private Map<String, Object> parseAsyncMessage(String message) {
        return readValueAsMapOfStringAndObject(message);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getResult(Map<String, Object> asyncMessage) {
        Object result = asyncMessage.get("result");
        if (result instanceof Map) {
            return (Map<String, Object>) result;
        }
        if (result instanceof List<?> list && !list.isEmpty()) {
            Object first = list.get(0);
            if (first instanceof Map) {
                return (Map<String, Object>) first;
            }
        }
        return null;
    }

    private HAStateManager createHAStateManagerForAssertion() {
        HAStateManager manager = HAStateManagerFactory.create(TEST_DB_TYPE);
        manager.initializeHA(HA_UUID, "FOR_ASSERTION", dbParams, dbHAConfig);
        return manager;
    }
}
