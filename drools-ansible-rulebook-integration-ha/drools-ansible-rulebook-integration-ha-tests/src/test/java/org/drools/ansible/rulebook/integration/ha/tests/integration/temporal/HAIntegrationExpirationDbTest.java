package org.drools.ansible.rulebook.integration.ha.tests.integration.temporal;

import org.drools.ansible.rulebook.integration.ha.tests.support.TestUtils;

import org.drools.ansible.rulebook.integration.ha.tests.integration.HAIntegrationTestBase;

import org.drools.ansible.rulebook.integration.ha.api.HAStateManager;
import org.drools.ansible.rulebook.integration.ha.model.SessionState;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.drools.ansible.rulebook.integration.ha.tests.support.TestUtils.createEvent;

/**
 * Integration tests verifying that event expiration in Drools working memory
 * is correctly reflected in the database SessionState.
 *
 * When a partial matching event expires (due to TTL), it is removed from
 * in-memory trackedRecords via the WorkingMemoryActionListener hook
 * (Drools does not fire objectDeleted for TTL expirations).
 * The database partial_matching_events column is updated on the next
 * persist cycle (triggered by advanceTime, assertEvent, etc.).
 */
class HAIntegrationExpirationDbTest extends HAIntegrationTestBase {

    // Rule that requires two events to match: i==2 AND j==m_0.i
    // If only one event arrives, it stays as a partial match until TTL expires.
    private static final String RULE_SET_TWO_EVENT_CORRELATION = """
            {
                "name": "Test Ruleset",
                "sources": {"EventSource": "test"},
                "rules": [
                    {
                        "Rule": {
                            "condition": {
                                "AllCondition": [
                                    {
                                        "EqualsExpression": {
                                            "lhs": {
                                                "Event": "i"
                                            },
                                            "rhs": {
                                                "Integer": 2
                                            }
                                        }
                                    },
                                    {
                                        "EqualsExpression": {
                                            "lhs": {
                                                "Event": "j"
                                            },
                                            "rhs": {
                                                "Event": "m_0.i"
                                            }
                                        }
                                    }
                                ]
                            },
                            "enabled": true,
                            "name": "correlation_rule"
                        }
                    }
                ]
            }
            """;

    @Override
    protected String getRuleSet() {
        return RULE_SET_TWO_EVENT_CORRELATION;
    }

    /**
     * Verifies that when a partial event expires from working memory,
     * the database SessionState's partial_matching_events is cleaned up
     * on the next persist cycle (advanceTime triggers persist).
     */
    @Test
    void testPartialEventRemovedFromDbAfterExpiration() {
        rulesEngine1.enableLeader();

        // Insert event that creates a partial match (waiting for {i:2})
        String event1 = createEvent("{ \"j\": 2 }");
        String result1 = rulesEngine1.assertEvent(sessionId1, event1);
        assertThat(result1).doesNotContain("correlation_rule");

        // DB should have 1 partial event
        assertPartialEventsInDb(1);

        // Advance to 1h59m — still within 2h default TTL
        rulesEngine1.advanceTime(sessionId1, 1, "HOURS");
        rulesEngine1.advanceTime(sessionId1, 59, "MINUTES");

        // DB should still have 1 partial event
        assertPartialEventsInDb(1);

        // Advance 2 more minutes (total 2h01m) — exceeds 2h TTL
        rulesEngine1.advanceTime(sessionId1, 2, "MINUTES");

        // DB should now have 0 partial events — expired event cleaned up
        assertPartialEventsInDb(0);

        // Working memory should also be empty
        String facts = rulesEngine1.getFacts(sessionId1);
        assertThat(facts).isEqualToNormalizingWhitespace("[]");
    }

    /**
     * Verifies that when multiple partial events exist, only the expired ones
     * are removed from the database while others remain.
     *
     * Event1 inserted at T=0, Event2 inserted at T=1h.
     * At T=2h01m, Event1 expires but Event2 is still alive.
     * At T=3h01m, Event2 also expires.
     */
    @Test
    void testMultipleEventsPartialExpirationInDb() {
        rulesEngine1.enableLeader();

        // T=0: Insert first event
        String event1 = createEvent("{ \"j\": 2 }");
        rulesEngine1.assertEvent(sessionId1, event1);

        // Advance to T=1h
        rulesEngine1.advanceTime(sessionId1, 1, "HOURS");

        // T=1h: Insert second event
        String event2 = createEvent("{ \"j\": 3 }");
        rulesEngine1.assertEvent(sessionId1, event2);

        // DB should have 2 partial events
        assertPartialEventsInDb(2);

        // Advance to T=2h01m — Event1 (inserted at T=0) expires, Event2 (inserted at T=1h) survives
        rulesEngine1.advanceTime(sessionId1, 1, "HOURS");
        rulesEngine1.advanceTime(sessionId1, 1, "MINUTES");

        // DB should have 1 partial event (only Event2 remains)
        assertPartialEventsInDb(1);

        // Verify Event2 is still in working memory
        String facts = rulesEngine1.getFacts(sessionId1);
        assertThat(facts).containsIgnoringWhitespaces("\"j\":3");

        // Advance to T=3h01m — Event2 (inserted at T=1h) also expires
        rulesEngine1.advanceTime(sessionId1, 1, "HOURS");

        // DB should have 0 partial events
        assertPartialEventsInDb(0);

        // Working memory is empty
        facts = rulesEngine1.getFacts(sessionId1);
        assertThat(facts).isEqualToNormalizingWhitespace("[]");
    }

    /**
     * Verifies that after failover, the recovered session correctly reflects
     * event expiration in the database.
     *
     * Node1 processes event, fails at T=1h59m.
     * Node2 recovers, advances past TTL, and DB is updated.
     */
    @Test
    void testExpiredEventRemovedFromDbAfterFailover() {
        rulesEngine1.enableLeader();

        // Insert event that creates a partial match
        String event1 = createEvent("{ \"j\": 2 }");
        rulesEngine1.assertEvent(sessionId1, event1);

        // DB should have 1 partial event
        assertPartialEventsInDb(1);

        // Advance Node1 to T=1h59m
        rulesEngine1.advanceTime(sessionId1, 1, "HOURS");
        rulesEngine1.advanceTime(sessionId1, 59, "MINUTES");

        // DB should still have 1 partial event
        assertPartialEventsInDb(1);

        // Sync Node2 clock before failover
        rulesEngine2.advanceTime(sessionId2, 60 + 59, "MINUTES");

        // === FAILOVER ===
        rulesEngine1.disableLeader();
        rulesEngine1.dispose(sessionId1);

        rulesEngine2.enableLeader();

        // After recovery, DB should still have 1 partial event (within TTL)
        assertPartialEventsInDb(1);

        // Node2: Advance 2 more minutes (total 2h01m) — exceeds TTL
        rulesEngine2.advanceTime(sessionId2, 2, "MINUTES");

        // DB should now have 0 partial events
        assertPartialEventsInDb(0);

        // Working memory is empty
        String facts = rulesEngine2.getFacts(sessionId2);
        assertThat(facts).isEqualToNormalizingWhitespace("[]");
    }

    private void assertPartialEventsInDb(int expectedSize) {
        HAStateManager haManager = createHAStateManagerForAssertion();
        try {
            SessionState state = haManager.getPersistedSessionState(getRuleSetNameValue());
            assertThat(state).isNotNull();
            assertThat(state.getPartialEvents()).hasSize(expectedSize);
        } finally {
            haManager.shutdown();
        }
    }
}
