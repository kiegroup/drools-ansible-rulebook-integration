package org.drools.ansible.rulebook.integration.ha.tests;

import java.util.List;
import java.util.Map;

import org.drools.ansible.rulebook.integration.ha.api.HAStateManager;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManagerFactory;
import org.drools.ansible.rulebook.integration.ha.model.MatchingEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.drools.ansible.rulebook.integration.ha.tests.TestUtils.createMatchingEvent;

/**
 * Tests for HA failover scenarios
 */
class HAStateManagerFailoverTest extends HAStateManagerTestBase {

    private static final String HA_UUID = "failover-ha-1";

    private HAStateManager node1;
    private HAStateManager node2;
    private HAStateManager node3;

    @BeforeEach
    void setUp() {
        node1 = createNode("node-1");
        node2 = createNode("node-2");
        node3 = createNode("node-3");
    }

    private HAStateManager createNode(String workerName) {
        HAStateManager manager = HAStateManagerFactory.create(TEST_DB_TYPE);
        manager.initializeHA(HA_UUID, workerName, dbParams, dbHAConfig);
        return manager;
    }

    @AfterEach
    void tearDown() {
        // these shutdown may be redundant but safe
        node1.shutdown();
        node2.shutdown();
        node3.shutdown();

        cleanupDatabase();
    }



    @Test
    void testLeaderFailoverWithPendingActions() {
         // Node 1 becomes leader
        node1.enableLeader();

        // Create matching events with actions in progress
        MatchingEvent me = createMatchingEvent(HA_UUID, "rules", "alert",
                                               Map.of("alert", "critical"));
        String me1 = node1.addMatchingEvent(me);

        // Start action execution
        String actionData = "{\"name\":\"send_notification\",\"status\":\"running\",\"reference_id\":\"job-123\"}";

        node1.addActionInfo(me1, 0, actionData);

        // Simulate node 1 failure
        node1.disableLeader();
        node1.shutdown();

        // Node 2 takes over
        node2.enableLeader();

        // Node 2 should see pending actions
        List<MatchingEvent> pending = node2.getPendingMatchingEvents();
        assertThat(pending).hasSize(1);

        MatchingEvent recovered = pending.get(0);
        assertThat(recovered.getMeUuid()).isEqualTo(me1);

        // Check action was preserved
        assertThat(node2.actionInfoExists(me1, 0)).isTrue();
        String recoveredAction = node2.getActionInfo(me1, 0);
        assertThat(recoveredAction).isEqualTo(actionData);

        // Node 2 can complete the action
        String completedAction = "{\"name\":\"send_notification\",\"status\":\"success\",\"reference_id\":\"job-123\"}";
        node2.updateActionInfo(me1, 0, completedAction);

        // Clean up
        node2.deleteActionInfo(me1);
        node2.shutdown();
    }

    @Test
    public void testMultipleFailovers() {
        // Node 1 starts work
        node1.enableLeader();

        MatchingEvent matchingEvent1 = createMatchingEvent(HA_UUID, "rules", "rule1",
                                                           Map.of("data", "1"));
        String me1 = node1.addMatchingEvent(matchingEvent1);

        node1.disableLeader();

        // Node 2 takes over
        node2.enableLeader();
        List<MatchingEvent> pending2 = node2.getPendingMatchingEvents();
        assertThat(pending2).hasSize(1);

        // Node 2 creates more work
        MatchingEvent matchingEvent2 = createMatchingEvent(HA_UUID, "rules", "rule2",
                                                           Map.of("data", "2"));
        String me2 = node2.addMatchingEvent(matchingEvent2);
        node2.disableLeader();

        // Node 3 takes over
        node3.enableLeader();
        List<MatchingEvent> pending3 = node3.getPendingMatchingEvents();

        // Should see both MEs
        assertThat(pending3).hasSize(2);
        assertThat(pending3.stream().map(MatchingEvent::getMeUuid))
                .containsExactlyInAnyOrder(me1, me2);

        // Clean up
        node1.shutdown();
        node2.shutdown();
        node3.shutdown();
    }

    // TODO: Confirm if this is an expected result
    // We may want an additional mechanism to resolve conflicts
    @Test
    public void testSplitBrainRecovery() {
        // Both nodes think they're leader (split brain)
        node1.enableLeader();
        node2.enableLeader();

        // Both try to create MEs
        MatchingEvent matchingEvent1 = createMatchingEvent(HA_UUID, "rules", "rule1",
                                                           Map.of("from", "node1"));
        node1.addMatchingEvent(matchingEvent1);

        MatchingEvent matchingEvent2 = createMatchingEvent(HA_UUID, "rules", "rule2",
                                                           Map.of("from", "node2"));
        node2.addMatchingEvent(matchingEvent2);

        // Resolve split brain - node2 wins
        node1.disableLeader();

        // Node2 should see both MEs
        List<MatchingEvent> allEvents = node2.getPendingMatchingEvents();
        assertThat(allEvents).hasSize(2);

        // Clean up
        node1.shutdown();
        node2.shutdown();
    }

    @Test
    public void testActionRetryAfterFailure() {
        node1.enableLeader();

        // Create ME with failed action
        MatchingEvent me = createMatchingEvent(HA_UUID, "rules", "retry_rule",
                                               Map.of("retry", true));
        String meUuid = node1.addMatchingEvent(me);

        String failedActionData = "{\"name\":\"flaky_action\",\"status\":\"failed\",\"reference_id\":\"failed-job-1\"}";

        node1.addActionInfo(meUuid, 0, failedActionData);
        node1.disableLeader();

        // New leader retries
        node2.enableLeader();

        String failedAction = node2.getActionInfo(meUuid, 0);
        assertThat(failedAction).isEqualTo(failedActionData);

        // Retry the action
        String retryAction = "{\"name\":\"flaky_action\",\"status\":\"running\",\"reference_id\":\"retry-job-2\"}";
        node2.updateActionInfo(meUuid, 0, retryAction);

        // Eventually succeed
        String successAction = "{\"name\":\"flaky_action\",\"status\":\"success\",\"reference_id\":\"retry-job-2\"}";
        node2.updateActionInfo(meUuid, 0, successAction);

        // Clean up
        node2.deleteActionInfo(meUuid);
        node1.shutdown();
        node2.shutdown();
    }
}