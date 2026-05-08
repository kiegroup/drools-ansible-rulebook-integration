package org.drools.ansible.rulebook.integration.ha.tests;

import java.util.Map;

import org.drools.ansible.rulebook.integration.ha.api.HAStateManager;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManagerFactory;
import org.drools.ansible.rulebook.integration.ha.model.HAStats;
import org.drools.ansible.rulebook.integration.ha.model.MatchingEvent;
import org.drools.ansible.rulebook.integration.ha.model.SessionState;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.drools.ansible.rulebook.integration.ha.tests.TestUtils.createMatchingEvent;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * General function tests for HAStateManager
 *
 * For Action and MatchingEvent related tests see HAStateManagerActionTest
 * For Session related tests see HAStateManagerSessionTest
 */
class HAStateManagerTest extends HAStateManagerTestBase {

    private HAStateManager stateManager;
    private String haUuid;  // Make it non-static and generate per test
    private static final String LEADER_ID = "test-leader-1";
    private static final String RULE_SET_NAME = "testRuleset";

    @BeforeEach
    void setUp() {
        System.out.println("Setting up HAStateManager for test...");
        // Generate unique HA_UUID per test to ensure complete isolation
        haUuid = "test-ha-" + System.currentTimeMillis();
        stateManager = HAStateManagerFactory.create(TEST_DB_TYPE);
        stateManager.initializeHA(haUuid, LEADER_ID, dbParams, dbHAConfig);
    }

    @AfterEach
    void tearDown() {
        if (stateManager != null) {
            stateManager.shutdown();
        }

        cleanupDatabase();
        System.out.println("Torn down HAStateManager after test.");
    }

    @Test
    void testLeaderElection() {
        // Initially not a leader
        assertThat(stateManager.isLeader()).isFalse();

        // Enable leader mode
        stateManager.enableLeader();
        assertThat(stateManager.isLeader()).isTrue();

        // Disable leader mode
        stateManager.disableLeader();
        assertThat(stateManager.isLeader()).isFalse();
    }

    @Test
    void testNonLeaderCannotPersist() {
        // Not setting as leader
        SessionState sessionState = new SessionState();
        sessionState.setHaUuid(haUuid);
        sessionState.setRuleSetName(RULE_SET_NAME);

        // Should throw IllegalStateException
        assertThrows(IllegalStateException.class, () -> {
            stateManager.persistSessionState(sessionState);
        });
    }

    @Test
    void testHAStats() {
        // Test initial HA stats
        stateManager.refreshHAStats();
        HAStats stats = stateManager.getHAStats();
        assertThat(stats).isNotNull();
        assertThat(stats.getCurrentLeader()).isNull();
        assertThat(stats.getLeaderSwitches()).isEqualTo(0);
        assertThat(stats.getEventsProcessedInTerm()).isEqualTo(0);
        assertThat(stats.getActionsProcessedInTerm()).isEqualTo(0);
        assertThat(stats.getIncompleteMatchingEvents()).isEqualTo(0);
        assertThat(stats.getPartialEventsInMemory()).isEqualTo(0);
        assertThat(stats.getPartialFulfilledRules()).isEqualTo(0);

        // Enable leader and verify stats update
        stateManager.enableLeader();
        stats = stateManager.getHAStats();
        assertThat(stats.getCurrentLeader()).isEqualTo(LEADER_ID);
        assertThat(stats.getLeaderSwitches()).isEqualTo(1);
        assertThat(stats.getCurrentTermStartedAt()).isNotNull();

        // Process some events/actions and verify counters
        SessionState sessionState = new SessionState();
        sessionState.setHaUuid(haUuid);
        sessionState.setRuleSetName(RULE_SET_NAME);
        stateManager.persistSessionState(sessionState);

        MatchingEvent me = createMatchingEvent(haUuid, "test", "rule", Map.of("test", "data"));
        String meUuid = stateManager.addMatchingEvent(me);
        stateManager.addActionInfo(meUuid, 0, "{\"name\":\"test_action\",\"status\":\"running\"}");

        // Check updated stats - refreshHAStats() needed for incompleteMatchingEvents (DB query)
        stateManager.refreshHAStats();
        stats = stateManager.getHAStats();
        assertThat(stats.getEventsProcessedInTerm()).isEqualTo(0); // Events processed not incremented in this test
        assertThat(stats.getActionsProcessedInTerm()).isEqualTo(1);
        assertThat(stats.getIncompleteMatchingEvents()).isEqualTo(1);
        assertThat(stats.getPartialEventsInMemory()).isEqualTo(0);
        assertThat(stats.getPartialFulfilledRules()).isEqualTo(0);
    }
}
