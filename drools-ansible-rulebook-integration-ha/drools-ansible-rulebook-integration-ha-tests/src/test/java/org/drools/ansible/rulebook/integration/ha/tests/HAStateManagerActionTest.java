package org.drools.ansible.rulebook.integration.ha.tests;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.drools.ansible.rulebook.integration.ha.api.HAStateManager;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManagerFactory;
import org.drools.ansible.rulebook.integration.ha.model.MatchingEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.drools.ansible.rulebook.integration.ha.tests.TestUtils.createMatchingEvent;

/**
 * Action and MatchingEvent related tests for HAStateManager
 */
class HAStateManagerActionTest extends HAStateManagerTestBase {

    private HAStateManager stateManager;

    private static final String HA_UUID = "test-ha-1";
    private static final String LEADER_ID = "test-leader-1";

    @BeforeEach
    void setUp() {
        stateManager = HAStateManagerFactory.create(TEST_DB_TYPE);
        stateManager.initializeHA(HA_UUID, LEADER_ID, dbParams, dbHAConfig);
    }

    @AfterEach
    void tearDown() {
        if (stateManager != null) {
            stateManager.shutdown();
        }

        cleanupDatabase();
    }

    @Test
    void testActionManagement() {
        stateManager.enableLeader();

        // First create a matching event
        MatchingEvent me = createMatchingEvent(HA_UUID, "testRuleset", "testRule",
                                               Map.of("fact", "value"));
        String meUuid = stateManager.addMatchingEvent(me);

        // Add an action
        String actionData = "{\"name\":\"send_alert\",\"status\":4,\"start_time\":\"2024-01-01T10:00:00Z\"}";
        stateManager.addActionInfo(meUuid, 0, actionData);

        // Verify action exists
        assertThat(stateManager.actionInfoExists(meUuid, 0)).isTrue();
        assertThat(stateManager.actionInfoExists(meUuid, 1)).isFalse();

        assertThat(stateManager.getActionStatus(meUuid, 0)).isEqualTo("4");

        // Get action and verify
        String retrieved = stateManager.getActionInfo(meUuid, 0);
        assertThat(retrieved).isEqualTo(actionData);

        // Update action
        String updatedData = "{\"name\":\"send_alert\",\"status\":3,\"end_time\":\"2024-01-01T10:01:00Z\"}";
        stateManager.updateActionInfo(meUuid, 0, updatedData);

        // Verify update
        retrieved = stateManager.getActionInfo(meUuid, 0);
        assertThat(retrieved).isEqualTo(updatedData);
        assertThat(stateManager.getActionStatus(meUuid, 0)).isEqualTo("3");

        // Delete action
        stateManager.deleteActionInfo(meUuid);

        // Verify action is gone
        assertThat(stateManager.actionInfoExists(meUuid, 0)).isFalse();

        // Should not appear in pending events
        List<MatchingEvent> pending = stateManager.getPendingMatchingEvents();
        assertThat(pending).isEmpty();
    }

    @Test
    void testPendingMatchingEventsRecovery() {
        stateManager.enableLeader();

        // Create multiple matching events with different states
        MatchingEvent matchingEvent1 = createMatchingEvent(HA_UUID, "ruleset1", "rule1",
                                                           Map.of("event", "1"));
        String meUuid1 = stateManager.addMatchingEvent(matchingEvent1);

        MatchingEvent matchingEvent2 = createMatchingEvent(HA_UUID, "ruleset1", "rule2",
                                                           Map.of("event", "2"));
        String meUuid2 = stateManager.addMatchingEvent(matchingEvent2);

        // Add action for first ME (in progress)
        stateManager.addActionInfo(meUuid1, 0, "{\"status\":4}");

        // Get pending events (should include both)
        List<MatchingEvent> pending = stateManager.getPendingMatchingEvents();
        assertThat(pending).hasSize(2);
        assertThat(pending.stream().map(MatchingEvent::getMeUuid))
                .containsExactlyInAnyOrder(meUuid1, meUuid2);
    }

    @Test
    void testNonLeaderActionMutationsFailFast() {
        stateManager.enableLeader();

        MatchingEvent me = createMatchingEvent(HA_UUID, "testRuleset", "testRule",
                                               Map.of("fact", "value"));
        String meUuid = stateManager.addMatchingEvent(me);
        stateManager.addActionInfo(meUuid, 0, "{\"status\":4}");

        stateManager.disableLeader();

        assertThatThrownBy(() -> stateManager.updateActionInfo(meUuid, 0, "{\"status\":3}"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Cannot update action info - not leader");

        assertThatThrownBy(() -> stateManager.deleteActionInfo(meUuid))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Cannot delete action info - not leader");
    }
}
