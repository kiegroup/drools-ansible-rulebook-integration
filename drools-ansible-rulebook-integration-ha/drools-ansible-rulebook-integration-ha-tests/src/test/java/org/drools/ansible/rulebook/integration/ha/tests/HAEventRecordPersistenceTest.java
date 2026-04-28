package org.drools.ansible.rulebook.integration.ha.tests;

import java.util.List;

import org.drools.ansible.rulebook.integration.ha.api.HAStateManager;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManagerFactory;
import org.drools.ansible.rulebook.integration.ha.api.HAUtils;
import org.drools.ansible.rulebook.integration.ha.model.EventRecord;
import org.drools.ansible.rulebook.integration.ha.model.EventRecordChange;
import org.drools.ansible.rulebook.integration.ha.model.EventRecordEntry;
import org.drools.ansible.rulebook.integration.ha.model.SessionState;
import org.drools.ansible.rulebook.integration.ha.tests.support.TestUtils;
import org.drools.ansible.rulebook.integration.ha.tests.state.HAStateManagerTestBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class HAEventRecordPersistenceTest extends HAStateManagerTestBase {

    private static final String HA_UUID = "event-record-persistence";
    private static final String LEADER_ID = "event-record-leader";
    private static final String RULE_SET_NAME = "event-record-ruleset";

    private HAStateManager stateManager;

    @BeforeEach
    void setUp() {
        stateManager = HAStateManagerFactory.create(TEST_DB_TYPE);
        stateManager.initializeHA(HA_UUID, LEADER_ID, dbParams, dbHAConfig);
        stateManager.enableLeader();
    }

    @AfterEach
    void tearDown() {
        if (stateManager != null) {
            stateManager.shutdown();
        }
        cleanupDatabase();
    }

    @Test
    void persistsUpdatesDeletesAndLoadsEventRecordsInRecoveryOrder() {
        EventRecordEntry second = new EventRecordEntry(
                "event-2",
                1L,
                new EventRecord("{\"j\":2}", 1_000L, EventRecord.RecordType.EVENT));
        EventRecordEntry first = new EventRecordEntry(
                "event-1",
                0L,
                new EventRecord("{\"i\":1}", 1_000L, EventRecord.RecordType.EVENT));

        stateManager.persistEventRecordChanges(RULE_SET_NAME, List.of(
                EventRecordChange.upsert(second),
                EventRecordChange.upsert(first)));

        List<EventRecordEntry> loaded = stateManager.getPersistedEventRecords(RULE_SET_NAME);

        assertThat(loaded).extracting(EventRecordEntry::getRecordIdentifier)
                .containsExactly("event-1", "event-2");
        assertThat(loaded).extracting(EventRecordEntry::getRecordSequence)
                .containsExactly(0L, 1L);
        assertThat(loaded.get(0).getRecord().getEventJson()).isEqualTo("{\"i\":1}");

        EventRecordEntry updatedFirst = new EventRecordEntry(
                "event-1",
                0L,
                new EventRecord("{\"i\":10}", 1_000L, EventRecord.RecordType.EVENT));

        stateManager.persistEventRecordChanges(RULE_SET_NAME, List.of(
                EventRecordChange.upsert(updatedFirst),
                EventRecordChange.delete("event-2")));

        loaded = stateManager.getPersistedEventRecords(RULE_SET_NAME);

        assertThat(loaded).hasSize(1);
        assertThat(loaded.get(0).getRecordIdentifier()).isEqualTo("event-1");
        assertThat(loaded.get(0).getRecordSequence()).isEqualTo(0L);
        assertThat(loaded.get(0).getRecord().getEventJson()).isEqualTo("{\"i\":10}");
    }

    @Test
    void rowAwareSessionPersistClearsLegacyBlobAndLoadsRowsForRecovery() {
        EventRecord event1 = new EventRecord("{\"i\":1}", 1_000L, EventRecord.RecordType.EVENT);
        EventRecord event2 = new EventRecord("{\"j\":2}", 2_000L, EventRecord.RecordType.EVENT);
        EventRecordEntry entry1 = new EventRecordEntry("event-1", 0L, event1);
        EventRecordEntry entry2 = new EventRecordEntry("event-2", 1L, event2);
        List<EventRecordEntry> entries = List.of(entry1, entry2);

        SessionState sessionState = new SessionState();
        sessionState.setHaUuid(HA_UUID);
        sessionState.setRuleSetName(RULE_SET_NAME);
        sessionState.setLeaderId(LEADER_ID);
        sessionState.setRulebookHash("rulebook-sha");
        sessionState.setCreatedTime(100L);
        sessionState.setPersistedTime(2_000L);
        sessionState.setPartialEvents(List.of(event1, event2));
        sessionState.setEventRecordsManifestSHA(HAUtils.calculateEventRecordsManifestSHA(entries));
        sessionState.setCurrentStateSHA(HAUtils.calculateStateSHA(sessionState));

        stateManager.loadOrCreateHAStats();
        stateManager.persistSessionStateStatsEventRecordsAndMatchingEvents(sessionState,
                                                                           entries.stream().map(EventRecordChange::upsert).toList(),
                                                                           List.of());

        String legacyBlob = TestUtils.queryRawColumn(dbParams,
                                                     "SELECT COALESCE(partial_matching_events, '__NULL__') "
                                                             + "FROM drools_ansible_session_state WHERE ha_uuid = ?",
                                                     HA_UUID);
        String eventRecordRows = TestUtils.queryRawColumn(dbParams,
                                                          "SELECT COUNT(*) FROM drools_ansible_event_record WHERE ha_uuid = ?",
                                                          HA_UUID);

        assertThat(legacyBlob).isEqualTo("__NULL__");
        assertThat(eventRecordRows).isEqualTo("2");

        SessionState loaded = stateManager.getPersistedSessionState(RULE_SET_NAME);

        assertThat(loaded.getPartialEvents()).hasSize(2);
        assertThat(loaded.getPartialEvents()).extracting(EventRecord::getEventJson)
                .containsExactly("{\"i\":1}", "{\"j\":2}");
        assertThat(loaded.getEventRecordsManifestSHA()).isEqualTo(HAUtils.calculateEventRecordsManifestSHA(entries));
        assertThat(stateManager.verifySessionState(loaded)).isTrue();

        stateManager.persistSessionState(loaded);

        String legacyBlobAfterGenericUpsert = TestUtils.queryRawColumn(dbParams,
                                                                       "SELECT COALESCE(partial_matching_events, '__NULL__') "
                                                                               + "FROM drools_ansible_session_state WHERE ha_uuid = ?",
                                                                       HA_UUID);
        assertThat(legacyBlobAfterGenericUpsert).isEqualTo("__NULL__");
    }

    @Test
    void rowLoadFailsFastWhenEventRecordShaDoesNotMatchPayload() {
        EventRecordEntry tamperedShaEntry = new EventRecordEntry(
                "event-1",
                0L,
                new EventRecord("{\"i\":1}", 1_000L, EventRecord.RecordType.EVENT),
                "not-the-row-sha");

        stateManager.persistEventRecordChanges(RULE_SET_NAME, List.of(EventRecordChange.upsert(tamperedShaEntry)));

        assertThatThrownBy(() -> stateManager.getPersistedEventRecords(RULE_SET_NAME))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("EventRecord SHA mismatch");
    }

    @Test
    void sessionVerificationFailsFastWhenEventRecordManifestNoLongerMatchesSessionSha() {
        EventRecord event1 = new EventRecord("{\"i\":1}", 1_000L, EventRecord.RecordType.EVENT);
        EventRecordEntry entry1 = new EventRecordEntry("event-1", 0L, event1);

        SessionState sessionState = new SessionState();
        sessionState.setHaUuid(HA_UUID);
        sessionState.setRuleSetName(RULE_SET_NAME);
        sessionState.setLeaderId(LEADER_ID);
        sessionState.setRulebookHash("rulebook-sha");
        sessionState.setCreatedTime(100L);
        sessionState.setPersistedTime(1_000L);
        sessionState.setPartialEvents(List.of(event1));
        sessionState.setEventRecordsManifestSHA(HAUtils.calculateEventRecordsManifestSHA(List.of(entry1)));
        sessionState.setCurrentStateSHA(HAUtils.calculateStateSHA(sessionState));

        stateManager.loadOrCreateHAStats();
        stateManager.persistSessionStateStatsEventRecordsAndMatchingEvents(sessionState,
                                                                           List.of(EventRecordChange.upsert(entry1)),
                                                                           List.of());

        EventRecord updatedEvent = new EventRecord("{\"i\":10}", 1_000L, EventRecord.RecordType.EVENT);
        EventRecordEntry updatedEntry = new EventRecordEntry("event-1", 0L, updatedEvent);
        stateManager.persistEventRecordChanges(RULE_SET_NAME, List.of(EventRecordChange.upsert(updatedEntry)));

        SessionState loaded = stateManager.getPersistedSessionState(RULE_SET_NAME);

        assertThat(loaded.getPartialEvents()).extracting(EventRecord::getEventJson).containsExactly("{\"i\":10}");
        assertThatThrownBy(() -> stateManager.verifySessionState(loaded))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("SessionState integrity check failed");
    }

    @Test
    void legacyBlobBackedStateMigratesToRowsOnNextRowAwarePersist() {
        EventRecord event1 = new EventRecord("{\"i\":1}", 1_000L, EventRecord.RecordType.EVENT);
        EventRecord event2 = new EventRecord("{\"j\":2}", 2_000L, EventRecord.RecordType.EVENT);

        SessionState legacyState = new SessionState();
        legacyState.setHaUuid(HA_UUID);
        legacyState.setRuleSetName(RULE_SET_NAME);
        legacyState.setLeaderId(LEADER_ID);
        legacyState.setRulebookHash("rulebook-sha");
        legacyState.setCreatedTime(100L);
        legacyState.setPersistedTime(2_000L);
        legacyState.setPartialEvents(List.of(event1, event2));
        legacyState.setEventRecordsManifestSHA(null);
        legacyState.setCurrentStateSHA(HAUtils.calculateStateSHA(legacyState));

        stateManager.persistSessionState(legacyState);

        String legacyBlobBeforeMigration = TestUtils.queryRawColumn(dbParams,
                                                                    "SELECT COALESCE(partial_matching_events, '__NULL__') "
                                                                            + "FROM drools_ansible_session_state WHERE ha_uuid = ?",
                                                                    HA_UUID);
        String rowsBeforeMigration = TestUtils.queryRawColumn(dbParams,
                                                              "SELECT COUNT(*) FROM drools_ansible_event_record WHERE ha_uuid = ?",
                                                              HA_UUID);
        assertThat(legacyBlobBeforeMigration).isNotEqualTo("__NULL__");
        assertThat(rowsBeforeMigration).isEqualTo("0");

        SessionState loadedLegacyState = stateManager.getPersistedSessionState(RULE_SET_NAME);
        assertThat(loadedLegacyState.getEventRecordsManifestSHA()).isNull();
        assertThat(loadedLegacyState.getPartialEvents()).hasSize(2);
        assertThat(stateManager.verifySessionState(loadedLegacyState)).isTrue();

        EventRecordEntry entry1 = new EventRecordEntry("event-1", 0L, event1);
        EventRecordEntry entry2 = new EventRecordEntry("event-2", 1L, event2);
        List<EventRecordEntry> entries = List.of(entry1, entry2);
        loadedLegacyState.setEventRecordsManifestSHA(HAUtils.calculateEventRecordsManifestSHA(entries));
        loadedLegacyState.setCurrentStateSHA(HAUtils.calculateStateSHA(loadedLegacyState));

        stateManager.persistSessionStateEventRecordsAndMatchingEvents(loadedLegacyState,
                                                                      entries.stream().map(EventRecordChange::upsert).toList(),
                                                                      List.of());

        String legacyBlobAfterMigration = TestUtils.queryRawColumn(dbParams,
                                                                   "SELECT COALESCE(partial_matching_events, '__NULL__') "
                                                                           + "FROM drools_ansible_session_state WHERE ha_uuid = ?",
                                                                   HA_UUID);
        String rowsAfterMigration = TestUtils.queryRawColumn(dbParams,
                                                             "SELECT COUNT(*) FROM drools_ansible_event_record WHERE ha_uuid = ?",
                                                             HA_UUID);
        assertThat(legacyBlobAfterMigration).isEqualTo("__NULL__");
        assertThat(rowsAfterMigration).isEqualTo("2");

        SessionState migratedState = stateManager.getPersistedSessionState(RULE_SET_NAME);
        assertThat(migratedState.getEventRecordsManifestSHA()).isEqualTo(HAUtils.calculateEventRecordsManifestSHA(entries));
        assertThat(migratedState.getPartialEvents()).extracting(EventRecord::getEventJson)
                .containsExactly("{\"i\":1}", "{\"j\":2}");
        assertThat(stateManager.verifySessionState(migratedState)).isTrue();
    }
}
