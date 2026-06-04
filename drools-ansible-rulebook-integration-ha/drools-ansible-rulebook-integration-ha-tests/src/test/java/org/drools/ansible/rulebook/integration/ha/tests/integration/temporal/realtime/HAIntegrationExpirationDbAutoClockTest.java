package org.drools.ansible.rulebook.integration.ha.tests.integration.temporal.realtime;

import org.drools.ansible.rulebook.integration.ha.tests.support.TestUtils;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.drools.ansible.rulebook.integration.core.jpy.AstRulesEngine;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManager;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManagerFactory;
import org.drools.ansible.rulebook.integration.ha.model.SessionState;
import org.drools.ansible.rulebook.integration.ha.tests.support.AbstractHATestBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.drools.ansible.rulebook.integration.ha.tests.support.TestUtils.createEvent;

/**
 * Realtime (AutoClock) tests verifying that event expiration cleans up
 * partial_matching_events in the database without manual clock advancement.
 *
 * Uses a short default_events_ttl (3 seconds) so tests complete quickly.
 *
 * Note: pure TTL expiration does not trigger a DB persist by itself.
 * After waiting for expiration, we insert a new event to trigger the
 * persist cycle and then verify the DB reflects the cleanup.
 */
class HAIntegrationExpirationDbAutoClockTest extends AbstractHATestBase {

    private static final String HA_UUID = "autoclock-expiration-db-ha-1";
    private static final String RULE_SET_NAME = "Expiration AutoClock Ruleset";

    // Two-event correlation rule with 3-second TTL
    private static final String RULE_SET_SHORT_TTL = """
            {
                "name": "Expiration AutoClock Ruleset",
                "rules": [
                    {
                        "Rule": {
                            "condition": {
                                "AllCondition": [
                                    {
                                        "EqualsExpression": {
                                            "lhs": { "Event": "i" },
                                            "rhs": { "Integer": 2 }
                                        }
                                    },
                                    {
                                        "EqualsExpression": {
                                            "lhs": { "Event": "j" },
                                            "rhs": { "Event": "m_0.i" }
                                        }
                                    }
                                ]
                            },
                            "enabled": true,
                            "name": "correlation_rule"
                        }
                    }
                ],
                "default_events_ttl": "3 seconds"
            }
            """;

    static {
        if (USE_POSTGRES) {
            initializePostgres("eda_ha_expiration_autoclock_test", "HA expiration auto-clock tests");
        } else {
            initializeH2();
        }
    }

    private AstRulesEngine rulesEngine1;
    private long sessionId1;
    private AsyncConsumer consumer1;

    @BeforeEach
    void setUp() {
        rulesEngine1 = new AstRulesEngine();
        consumer1 = new AsyncConsumer("consumer1");
        consumer1.startConsuming(rulesEngine1.port());

        rulesEngine1.initializeHA(HA_UUID, "worker-1", dbParamsJson, dbHAConfigJson);
        // No FULLY_MANUAL_PSEUDOCLOCK — auto-clock runs naturally
        sessionId1 = rulesEngine1.createRuleset(RULE_SET_SHORT_TTL);
    }

    @AfterEach
    void tearDown() {
        if (consumer1 != null) consumer1.stop();
        if (rulesEngine1 != null) {
            rulesEngine1.dispose(sessionId1);
            rulesEngine1.close();
        }
        cleanupDatabase();
    }

    /**
     * Verifies that when a partial event expires via the auto-clock,
     * the expired event is removed from in-memory trackedRecords and
     * the database reflects the cleanup after the next persist cycle.
     */
    @Test
    void testPartialEventRemovedFromDbAfterAutoClockExpiration() throws InterruptedException {
        rulesEngine1.enableLeader();

        // Insert event that creates a partial match (waiting for {i:2})
        String event1 = createEvent("{ \"j\": 2 }");
        String result1 = rulesEngine1.assertEvent(sessionId1, event1);
        assertThat(result1).doesNotContain("correlation_rule");

        // DB should have 1 partial event
        assertPartialEventsInDb(1);

        // Wait for auto-clock to expire the event (3s TTL + margin)
        Thread.sleep(4000);

        // Working memory should be empty after expiration
        String facts = rulesEngine1.getFacts(sessionId1);
        assertThat(facts).isEqualToNormalizingWhitespace("[]");

        // Insert a new event to trigger a persist cycle
        // (pure TTL expiration alone does not trigger DB persist)
        String event2 = createEvent("{ \"j\": 99 }");
        rulesEngine1.assertEvent(sessionId1, event2);

        // DB should have only the new event, not the expired one
        assertPartialEventsInDb(1);
    }

    /**
     * Verifies that with multiple partial events inserted at different times,
     * the auto-clock expires them independently and the DB reflects each removal
     * after a persist cycle is triggered.
     */
    @Test
    void testMultipleEventsExpireIndependentlyWithAutoClock() throws InterruptedException {
        rulesEngine1.enableLeader();

        // Insert first event at T=0
        String event1 = createEvent("{ \"j\": 2 }");
        rulesEngine1.assertEvent(sessionId1, event1);
        assertPartialEventsInDb(1);

        // Wait 2 seconds, then insert second event (within TTL of first)
        Thread.sleep(2000);

        String event2 = createEvent("{ \"j\": 3 }");
        rulesEngine1.assertEvent(sessionId1, event2);
        assertPartialEventsInDb(2);

        // Wait for first event to expire (~1s more for 3s TTL) + margin
        Thread.sleep(2000);

        // Insert a dummy event to trigger persist — first event should be gone
        String event3 = createEvent("{ \"j\": 99 }");
        rulesEngine1.assertEvent(sessionId1, event3);

        // DB should have 2 partial events: event2 (still alive) + event3 (just inserted)
        assertPartialEventsInDb(2);

        // Wait for event2 and event3 to expire (3s TTL + margin)
        Thread.sleep(4000);

        // Insert another dummy event to trigger persist — all previous events should be gone
        String event4 = createEvent("{ \"j\": 100 }");
        rulesEngine1.assertEvent(sessionId1, event4);

        // Only event4 should remain
        assertPartialEventsInDb(1);
    }

    private void assertPartialEventsInDb(int expectedSize) {
        HAStateManager haManager = HAStateManagerFactory.create(TEST_DB_TYPE);
        haManager.initializeHA(HA_UUID, "FOR_ASSERTION", dbParams, dbHAConfig);
        try {
            SessionState state = haManager.getPersistedSessionState(RULE_SET_NAME);
            assertThat(state).isNotNull();
            assertThat(state.getPartialEvents()).hasSize(expectedSize);
        } finally {
            haManager.shutdown();
        }
    }

    static class AsyncConsumer {
        private volatile boolean keepReading = true;
        private final ExecutorService executor = Executors.newSingleThreadExecutor();
        private final String name;
        private final List<String> receivedMessages = new CopyOnWriteArrayList<>();

        AsyncConsumer(String name) {
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
    }
}
