package org.drools.ansible.rulebook.integration.ha.tests.integration.temporal;

import org.drools.ansible.rulebook.integration.ha.tests.support.TestUtils;

import org.drools.ansible.rulebook.integration.ha.tests.integration.HAIntegrationTestBase;

import org.drools.ansible.rulebook.integration.ha.api.HAStateManager;
import org.drools.ansible.rulebook.integration.ha.model.SessionState;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.drools.ansible.rulebook.integration.ha.tests.support.TestUtils.createEvent;

/**
 * Integration tests for event expiration/eviction in HA mode.
 * Verifies that event TTL (Time-To-Live) is properly maintained across failover scenarios.
 *
 * Based on AutoEvictTest but extended to test that:
 * 1. Event insertion time is persisted to database
 * 2. Clock time is persisted and restored during failover
 * 3. TTL expiration calculations work correctly after node recovery
 */
class HAIntegrationExpirationTest extends HAIntegrationTestBase {

    // Rule that requires two events to match: i==2 AND j==i
    // If only one event arrives, it should expire based on TTL
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

    private static final String RULE_SET_WITH_CUSTOM_TTL = """
            {
                "name": "Test Ruleset with Custom TTL",
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
                ],
                "default_events_ttl": "10 hours"
            }
            """;

    @Override
    protected String getRuleSet() {
        return RULE_SET_TWO_EVENT_CORRELATION;
    }

    /**
     * Test that events expire after default TTL (2 hours) across failover.
     *
     * Scenario:
     * 1. Node1 is leader, processes event that doesn't match (partial correlation)
     * 2. Time advances on Node1 (1 hour, then 59 min) - event should persist in SessionState
     * 3. Node1 fails, Node2 becomes leader and recovers session
     * 4. Time advances on Node2 (2 more minutes = total 2h01m) - event should expire
     *
     * This verifies:
     * - Event insertion time is persisted to database
     * - Clock time is persisted and restored during failover
     * - TTL calculation works correctly after recovery
     */
    @Test
    void testEvictEventsUsingDefaultExpiration() {
        // Node1: Enable leader mode
        rulesEngine1.enableLeader();

        // Node1: Process event that doesn't match (waiting for Event{i:2})
        // This creates a partial event waiting for correlation
        String event1 = createEvent("{ \"j\": 2 }");
        String result1 = rulesEngine1.assertEvent(sessionId1, event1);
        assertThat(result1).doesNotContain("correlation_rule");

        // Verify event is persisted as partial event in SessionState
        HAStateManager haManager1 = createHAStateManagerForAssertion();
        SessionState state1 = haManager1.getPersistedSessionState(getRuleSetNameValue());
        assertThat(state1).isNotNull();
        assertThat(state1.getPartialEvents()).hasSize(1);
        haManager1.shutdown();

        // Node1: Advance time by 1 hour - event should still be in SessionState
        rulesEngine1.advanceTime(sessionId1, 1, "HOURS");

        // Node1: Advance time by 59 minutes (total: 1h59m) - event should still be there
        rulesEngine1.advanceTime(sessionId1, 59, "MINUTES");

        // Node1: confirm event1 is still in memory
        String facts1 = rulesEngine1.getFacts(sessionId1);
        assertThat(facts1).containsIgnoringWhitespaces("\"j\":2");

        // Node2 should have the same time as Node1
        // This is important to advance time correctly during recoverSession
        rulesEngine2.advanceTime(sessionId2, 60 + 59, "MINUTES");

        // === FAILOVER SCENARIO ===
        // Node1 fails, Node2 takes over and recovers the session
        rulesEngine1.disableLeader();
        rulesEngine1.dispose(sessionId1);

        rulesEngine2.enableLeader();

        // Node2: confirm event1 is still in memory
        String facts2 = rulesEngine2.getFacts(sessionId2);
        assertThat(facts2).containsIgnoringWhitespaces("\"j\":2");

        // Node2: Advance time by 2 more minutes (total: 2h01m)
        // Event should now expire (exceeded 2 hour default TTL)
        rulesEngine2.advanceTime(sessionId2, 2, "MINUTES");

        // Node2: confirm event1 no longer in memory
        facts2 = rulesEngine2.getFacts(sessionId2);
        assertThat(facts2).isEqualToNormalizingWhitespace("[]");

        // After 2h01m, the event should have expired and been removed. So event2 should not match.
        String event2 = createEvent("{ \"i\": 2 }");
        String result2 = rulesEngine2.assertEvent(sessionId2, event2);
        assertThat(result2).doesNotContain("correlation_rule");
    }

    /**
     * Test that events expire after custom TTL (10 hours) across failover.
     *
     * Scenario:
     * 1. Node1 is leader, processes event with custom 10-hour TTL
     * 2. Time advances on Node1 (1h, 59min, 2min = 2h01m) - event should persist
     * 3. Node1 fails, Node2 becomes leader and recovers session
     * 4. Time advances on Node2 (8 more hours = total 10h01m) - event should expire
     *
     * This verifies:
     * - Custom TTL configuration is respected
     * - TTL calculations work correctly with custom values across failover
     */
    @Test
    void testEvictEventsUsingGivenExpiration() {
        // Override with custom TTL ruleset
        sessionId1 = rulesEngine1.createRuleset(RULE_SET_WITH_CUSTOM_TTL);
        sessionId2 = rulesEngine2.createRuleset(RULE_SET_WITH_CUSTOM_TTL);

        String customRuleSetName = "Test Ruleset with Custom TTL"; // Same name in RULE_SET_WITH_CUSTOM_TTL

        // Node1: Enable leader mode
        rulesEngine1.enableLeader();

        // Node1: Process event that doesn't match
        String event1 = createEvent("{ \"j\": 2 }");
        String result1 = rulesEngine1.assertEvent(sessionId1, event1);
        assertThat(result1).doesNotContain("correlation_rule");

        // Verify event is persisted as partial event
        HAStateManager haManager1 = createHAStateManagerForAssertion();
        SessionState state1 = haManager1.getPersistedSessionState(customRuleSetName);
        assertThat(state1).isNotNull();
        assertThat(state1.getPartialEvents()).hasSize(1);

        haManager1.shutdown();

        // Node1: Advance time by 1 hour - event should still be there (TTL: 10 hours)
        rulesEngine1.advanceTime(sessionId1, 1, "HOURS");

        // Node1: Advance time by 59 minutes (total: 1h59m) - event should still be there
        rulesEngine1.advanceTime(sessionId1, 59, "MINUTES");

        // Node1: Advance time by 2 minutes (total: 2h01m)
        // Unlike default TTL test, event should STILL be there (10h TTL)
        rulesEngine1.advanceTime(sessionId1, 2, "MINUTES");

        // Node1: confirm event1 is still in memory (not expired yet, 10h TTL)
        String facts1 = rulesEngine1.getFacts(sessionId1);
        assertThat(facts1).containsIgnoringWhitespaces("\"j\":2");

        // Node2 should have the same time as Node1
        // This is important to advance time correctly during recoverSession
        rulesEngine2.advanceTime(sessionId2, 60 + 59 + 2, "MINUTES");

        // === FAILOVER SCENARIO ===
        // Node1 fails, Node2 takes over
        rulesEngine1.disableLeader();
        rulesEngine1.dispose(sessionId1);

        rulesEngine2.enableLeader();

        // Node2: confirm event1 is still in memory after recovery (still within 10h TTL)
        String facts2 = rulesEngine2.getFacts(sessionId2);
        assertThat(facts2).containsIgnoringWhitespaces("\"j\":2");

        // Node2: Advance time by 8 more hours (total: 10h01m)
        // Event should now expire (exceeded 10 hour custom TTL)
        rulesEngine2.advanceTime(sessionId2, 8, "HOURS");

        // Node2: confirm event1 no longer in memory (expired after 10h01m)
        facts2 = rulesEngine2.getFacts(sessionId2);
        assertThat(facts2).isEqualToNormalizingWhitespace("[]");

        // After 10h01m, the event should have expired and been removed. So event2 should not match.
        String event2 = createEvent("{ \"i\": 2 }");
        String result2 = rulesEngine2.assertEvent(sessionId2, event2);
        assertThat(result2).doesNotContain("correlation_rule");
    }
}
