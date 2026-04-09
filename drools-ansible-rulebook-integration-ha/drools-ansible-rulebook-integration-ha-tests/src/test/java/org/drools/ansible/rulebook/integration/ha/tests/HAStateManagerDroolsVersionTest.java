package org.drools.ansible.rulebook.integration.ha.tests;

import java.util.List;
import java.util.Map;

import org.drools.ansible.rulebook.integration.ha.api.HAStateManager;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManagerFactory;
import org.drools.ansible.rulebook.integration.ha.api.HAUtils;
import org.drools.ansible.rulebook.integration.ha.model.HAStats;
import org.drools.ansible.rulebook.integration.ha.model.MatchingEvent;
import org.drools.ansible.rulebook.integration.ha.model.SessionState;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.drools.ansible.rulebook.integration.ha.tests.TestUtils.createMatchingEvent;

/**
 * Verifies that drools_version is automatically injected into the metadata
 * JSONB column for all 4 HA tables (session_state, matching_event, action_info, ha_stats).
 */
class HAStateManagerDroolsVersionTest extends HAStateManagerTestBase {

    private static final String DROOLS_VERSION_KEY = "drools_version";
    private static final String EXPECTED_VERSION = "ha-poc-0.0.8";

    private HAStateManager stateManager;
    private static final String HA_UUID = "test-ha-version";
    private static final String LEADER_ID = "test-leader-1";
    private static final String RULE_SET_NAME = "VersionTestRuleset";

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
    void testSessionStateMetadataContainsDroolsVersion() {
        stateManager.enableLeader();

        SessionState sessionState = new SessionState();
        sessionState.setHaUuid(HA_UUID);
        sessionState.setRuleSetName(RULE_SET_NAME);
        sessionState.setRulebookHash("hash-001");
        sessionState.setLeaderId(LEADER_ID);
        sessionState.setPartialEvents(List.of());
        long now = System.currentTimeMillis();
        sessionState.setCreatedTime(now);
        sessionState.setPersistedTime(now);
        sessionState.setCurrentStateSHA(HAUtils.calculateStateSHA(sessionState));

        stateManager.persistSessionState(sessionState);

        SessionState retrieved = stateManager.getPersistedSessionState(RULE_SET_NAME);
        assertThat(retrieved).isNotNull();
        assertThat(retrieved.getMetadata()).containsEntry(DROOLS_VERSION_KEY, EXPECTED_VERSION);
    }

    @Test
    void testMatchingEventMetadataContainsDroolsVersion() {
        stateManager.enableLeader();

        MatchingEvent me = createMatchingEvent(HA_UUID, RULE_SET_NAME, "testRule",
                                               Map.of("key", "value"));
        String meUuid = stateManager.addMatchingEvent(me);
        assertThat(meUuid).isNotNull();

        List<MatchingEvent> pending = stateManager.getPendingMatchingEvents();
        assertThat(pending).hasSize(1);
        assertThat(pending.get(0).getMetadata()).containsEntry(DROOLS_VERSION_KEY, EXPECTED_VERSION);
    }

    @Test
    void testActionInfoMetadataContainsDroolsVersion() {
        stateManager.enableLeader();

        MatchingEvent me = createMatchingEvent(HA_UUID, RULE_SET_NAME, "testRule",
                                               Map.of("key", "value"));
        String meUuid = stateManager.addMatchingEvent(me);

        stateManager.addActionInfo(meUuid, 0, "{\"status\":4}");

        // Verify via raw database query that the metadata column contains drools_version
        String rawMetadata = TestUtils.queryRawColumn(dbParams,
                "SELECT metadata FROM drools_ansible_action_info WHERE ha_uuid = ?",
                HA_UUID);
        assertThat(rawMetadata).contains(DROOLS_VERSION_KEY);
        assertThat(rawMetadata).contains(EXPECTED_VERSION);
    }

    @Test
    void testHAStatsMetadataContainsDroolsVersion() {
        stateManager.enableLeader();

        HAStats stats = stateManager.getHAStats();
        assertThat(stats).isNotNull();
        assertThat(stats.getMetadata()).containsEntry(DROOLS_VERSION_KEY, EXPECTED_VERSION);
    }

    @Test
    void testDroolsVersionPreservesExistingMetadata() {
        stateManager.enableLeader();

        SessionState sessionState = new SessionState();
        sessionState.setHaUuid(HA_UUID);
        sessionState.setRuleSetName(RULE_SET_NAME);
        sessionState.setRulebookHash("hash-002");
        sessionState.setLeaderId(LEADER_ID);
        sessionState.setPartialEvents(List.of());
        long now = System.currentTimeMillis();
        sessionState.setCreatedTime(now);
        sessionState.setPersistedTime(now);

        // Pre-populate metadata with a custom key
        sessionState.getMetadata().put("custom_key", "custom_value");
        sessionState.setCurrentStateSHA(HAUtils.calculateStateSHA(sessionState));

        stateManager.persistSessionState(sessionState);

        SessionState retrieved = stateManager.getPersistedSessionState(RULE_SET_NAME);
        assertThat(retrieved).isNotNull();
        assertThat(retrieved.getMetadata())
                .containsEntry(DROOLS_VERSION_KEY, EXPECTED_VERSION)
                .containsEntry("custom_key", "custom_value");
    }
}
