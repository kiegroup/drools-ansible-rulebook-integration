package org.drools.ansible.rulebook.integration.ha.tests.integration.temporal.realtime;

import org.drools.ansible.rulebook.integration.ha.tests.support.TestUtils;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
import static org.drools.ansible.rulebook.integration.ha.tests.support.TestUtils.createEvent;

/**
 * Tests for AutomaticPseudoClock firing TimedOut rules in HA mode.
 * Unlike HAIntegrationTimedOutTest which uses FULLY_MANUAL_PSEUDOCLOCK,
 * these tests let the auto-clock run naturally so TimedOut windows expire via
 * the daemon thread's scheduledAdvanceTimeToMills() path.
 */
class HAIntegrationTimedOutAutoClockTest extends AbstractHATestBase {

    private static final String HA_UUID = "autoclock-timedout-ha-1";

    private static final String RULE_SET_TIMED_OUT_3S = """
            {
                "name": "TimedOut AutoClock Ruleset",
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
                            "timeout": "3 seconds"
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
            initializePostgres("eda_ha_timedout_autoclock_test", "HA TimedOut auto-clock tests");
        } else {
            initializeH2();
        }
    }

    private AstRulesEngine rulesEngine1;
    private AstRulesEngine rulesEngine2;
    private long sessionId1;
    private long sessionId2;
    private ThreadSafeAsyncConsumer consumer1;
    private ThreadSafeAsyncConsumer consumer2;

    @BeforeEach
    void setUp() {
        rulesEngine1 = new AstRulesEngine();
        consumer1 = new ThreadSafeAsyncConsumer("consumer1");
        consumer1.startConsuming(rulesEngine1.port());

        rulesEngine1.initializeHA(HA_UUID, "worker-1", dbParamsJson, dbHAConfigJson);
        // No FULLY_MANUAL_PSEUDOCLOCK — auto-clock runs naturally
        sessionId1 = rulesEngine1.createRuleset(RULE_SET_TIMED_OUT_3S);

        rulesEngine2 = new AstRulesEngine();
        consumer2 = new ThreadSafeAsyncConsumer("consumer2");
        consumer2.startConsuming(rulesEngine2.port());

        rulesEngine2.initializeHA(HA_UUID, "worker-2", dbParamsJson, dbHAConfigJson);
        sessionId2 = rulesEngine2.createRuleset(RULE_SET_TIMED_OUT_3S);
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
     * Verifies that when the auto-clock fires a TimedOut match on the leader,
     * the match goes through the HA pipeline: matching_uuid is present in the
     * async response and a MatchingEvent is persisted in the database.
     */
    @Test
    void testAutoClockFiresTimedOutOnLeader() {
        rulesEngine1.enableLeader();

        // Insert partial match (code=1001 only) — should not fire immediately
        String event = createEvent("{\"alert\":{\"code\":1001,\"message\":\"Applying maintenance\"}}");
        rulesEngine1.assertEvent(sessionId1, event);

        // Wait for auto-clock to fire the TimedOut rule (3s timeout + some margin)
        await().atMost(10, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .until(() -> !consumer1.getReceivedMessages().isEmpty());

        // Verify async response has matching_uuid
        List<String> messages = consumer1.getReceivedMessages();
        assertThat(messages).hasSize(1);

        Map<String, Object> response = parseAsyncMessage(messages.get(0));
        Map<String, Object> result = getResult(response);
        assertThat(result)
                .containsEntry("name", "maint failed")
                .containsKey("matching_uuid");
        String matchingUuid = (String) result.get("matching_uuid");
        assertThat(matchingUuid).isNotEmpty();

        // Verify MatchingEvent was persisted in database
        HAStateManager assertionManager = createHAStateManagerForAssertion();
        try {
            List<MatchingEvent> pendingEvents = assertionManager.getPendingMatchingEvents();
            assertThat(pendingEvents)
                    .extracting(MatchingEvent::getMeUuid)
                    .contains(matchingUuid);
        } finally {
            assertionManager.shutdown();
        }
    }

    /**
     * Insert event on Node1 leader, failover immediately (before 3s timeout expires),
     * Node2 recovers and its auto-clock fires the TimedOut match.
     */
    @Test
    void testFailoverBeforeTimeoutExpires() {
        rulesEngine1.enableLeader();

        // Insert partial match on Node1
        String event = createEvent("{\"alert\":{\"code\":1001,\"message\":\"Applying maintenance\"}}");
        rulesEngine1.assertEvent(sessionId1, event);

        // Failover immediately (before auto-clock fires)
        rulesEngine1.disableLeader();
        rulesEngine1.dispose(sessionId1);
        rulesEngine1.close();
        rulesEngine1 = null;
        consumer1.stop();
        consumer1 = null;

        // Node2 becomes leader and recovers session
        rulesEngine2.enableLeader();

        // Wait for auto-clock on Node2 to fire the TimedOut rule
        await().atMost(10, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .until(() -> {
                    // Filter out MATCHING_EVENT_RECOVERY messages — we want the auto-clock match
                    return consumer2.getReceivedMessages().stream()
                            .anyMatch(msg -> {
                                Map<String, Object> resp = parseAsyncMessage(msg);
                                Map<String, Object> r = getResult(resp);
                                return r != null
                                        && "maint failed".equals(r.get("name"))
                                        && !"MATCHING_EVENT_RECOVERY".equals(r.get("type"));
                            });
                });

        // Find the auto-clock match (not recovery)
        String autoClockMessage = consumer2.getReceivedMessages().stream()
                .filter(msg -> {
                    Map<String, Object> resp = parseAsyncMessage(msg);
                    Map<String, Object> r = getResult(resp);
                    return r != null
                            && "maint failed".equals(r.get("name"))
                            && !"MATCHING_EVENT_RECOVERY".equals(r.get("type"));
                })
                .findFirst()
                .orElseThrow();

        Map<String, Object> response = parseAsyncMessage(autoClockMessage);
        Map<String, Object> result = getResult(response);
        assertThat(result)
                .containsEntry("name", "maint failed")
                .containsKey("matching_uuid");
        String matchingUuid = (String) result.get("matching_uuid");
        assertThat(matchingUuid).isNotEmpty();
    }

    /**
     * Insert event on Node1, auto-clock fires on Node1 (HA-persisted),
     * then failover. Node2 recovers the pending MatchingEvent.
     */
    @Test
    void testAutoClockFiresThenFailover() {
        rulesEngine1.enableLeader();

        // Insert partial match on Node1
        String event = createEvent("{\"alert\":{\"code\":1001,\"message\":\"Applying maintenance\"}}");
        rulesEngine1.assertEvent(sessionId1, event);

        // Wait for auto-clock to fire on Node1
        await().atMost(10, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .until(() -> !consumer1.getReceivedMessages().isEmpty());

        // Extract matching_uuid from Node1's match
        Map<String, Object> node1Response = parseAsyncMessage(consumer1.getReceivedMessages().get(0));
        Map<String, Object> node1Result = getResult(node1Response);
        String matchingUuid = (String) node1Result.get("matching_uuid");
        assertThat(matchingUuid).isNotEmpty();

        // Failover
        rulesEngine1.disableLeader();
        rulesEngine1.dispose(sessionId1);
        rulesEngine1.close();
        rulesEngine1 = null;
        consumer1.stop();
        consumer1 = null;

        // Node2 becomes leader — should recover the pending MatchingEvent
        rulesEngine2.enableLeader();

        // Wait for recovery notification on consumer2
        await().atMost(10, TimeUnit.SECONDS)
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .until(() -> consumer2.getReceivedMessages().stream()
                        .anyMatch(msg -> {
                            Map<String, Object> resp = parseAsyncMessage(msg);
                            Map<String, Object> r = getResult(resp);
                            return r != null && "MATCHING_EVENT_RECOVERY".equals(r.get("type"));
                        }));

        // Verify recovery message contains the same matching_uuid
        String recoveryMessage = consumer2.getReceivedMessages().stream()
                .filter(msg -> {
                    Map<String, Object> resp = parseAsyncMessage(msg);
                    Map<String, Object> r = getResult(resp);
                    return r != null && "MATCHING_EVENT_RECOVERY".equals(r.get("type"));
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

    /**
     * Thread-safe async consumer using CopyOnWriteArrayList.
     */
    static class ThreadSafeAsyncConsumer {
        private volatile boolean keepReading = true;
        private final ExecutorService executor = Executors.newSingleThreadExecutor();
        private final String name;
        private final List<String> receivedMessages = new CopyOnWriteArrayList<>();

        ThreadSafeAsyncConsumer(String name) {
            this.name = name;
        }

        void startConsuming(int port) {
            executor.submit(() -> {
                try (Socket socket = new Socket("localhost", port)) {
                    socket.setSoTimeout(1000);
                    DataInputStream stream = new DataInputStream(socket.getInputStream());

                    while (keepReading && !Thread.currentThread().isInterrupted()) {
                        try {
                            if (stream.available() > 0) {
                                int l = stream.readInt();
                                byte[] bytes = stream.readNBytes(l);
                                String result = new String(bytes, StandardCharsets.UTF_8);
                                System.out.println(name + " - Async result: " + result);
                                receivedMessages.add(result);
                            }
                        } catch (SocketTimeoutException e) {
                            continue;
                        }
                    }
                } catch (IOException e) {
                    if (keepReading) {
                        System.err.println(name + " error: " + e.getMessage());
                    }
                }
            });
        }

        void stop() {
            keepReading = false;
            executor.shutdown();
            try {
                if (!executor.awaitTermination(2, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
            }
        }

        List<String> getReceivedMessages() {
            return receivedMessages;
        }
    }
}
