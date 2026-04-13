package org.drools.ansible.rulebook.integration.ha.tests;

import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManager;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManagerFactory;
import org.drools.ansible.rulebook.integration.ha.model.EventRecord;
import org.drools.ansible.rulebook.integration.ha.model.SessionState;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.drools.ansible.rulebook.integration.ha.api.HAUtils.calculateStateSHA;

class HAStateShaJsonRoundTripTest extends HAStateManagerTestBase {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final String HA_UUID = "sha-json-round-trip";
    private static final String LEADER_ID = "sha-json-round-trip-leader";
    private static final String RULE_SET_NAME = "sha-json-round-trip-ruleset";
    private static final String RULEBOOK_HASH = "rulebook-sha-json-round-trip";

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
    void stateShaRemainsStableAfterJsonRoundTripWithNumericPayloadAndLongFields() throws Exception {
        // This protects the SHA contract: eventJson must remain a raw string in
        // SessionState.toHashableContent(), not a parsed Map/object whose numeric
        // types or key order could drift after JSON round-tripping.
        long createdTime = 1_765_560_000_000L;
        long persistedTime = createdTime + 1_000L;
        long insertedAt = createdTime + 2_000L;
        Long expirationDuration = 3_600_000L;
        String eventJson = """
                {
                    "small_integer": 1,
                    "large_long": 9223372036854775806,
                    "floating_point": 1.25,
                    "nested": {
                        "counter": 2
                    }
                }
                """;

        EventRecord eventRecord = new EventRecord(
                eventJson,
                insertedAt,
                EventRecord.RecordType.CONTROL_ONCE_WITHIN,
                expirationDuration);

        SessionState sessionState = new SessionState();
        sessionState.setHaUuid(HA_UUID);
        sessionState.setRuleSetName(RULE_SET_NAME);
        sessionState.setRulebookHash(RULEBOOK_HASH);
        sessionState.setLeaderId(LEADER_ID);
        sessionState.setPartialEvents(List.of(eventRecord));
        sessionState.setProcessedEventIds(List.of("event-1"));
        sessionState.setCreatedTime(createdTime);
        sessionState.setPersistedTime(persistedTime);
        sessionState.setCurrentStateSHA(calculateStateSHA(sessionState));

        String persistedSha = sessionState.getCurrentStateSHA();
        assertHashableContentKeepsEventJsonAsRawString(sessionState, eventJson);
        stateManager.persistSessionState(sessionState);

        stateManager.shutdown();
        stateManager = HAStateManagerFactory.create(TEST_DB_TYPE);
        stateManager.initializeHA(HA_UUID, LEADER_ID, dbParams, dbHAConfig);

        SessionState loaded = stateManager.getPersistedSessionState(RULE_SET_NAME);

        assertThat(loaded.getCurrentStateSHA()).isEqualTo(persistedSha);
        assertThat(calculateStateSHA(loaded)).isEqualTo(persistedSha);
        assertThat(stateManager.verifySessionState(loaded)).isTrue();
        assertHashableContentKeepsEventJsonAsRawString(loaded, eventJson);

        EventRecord loadedRecord = loaded.getPartialEvents().get(0);
        assertThat(loadedRecord.getEventJson()).isEqualTo(eventJson);
        assertThat(loadedRecord.getInsertedAt()).isEqualTo(insertedAt);
        assertThat(loadedRecord.getExpirationDuration()).isEqualTo(expirationDuration);
    }

    private static void assertHashableContentKeepsEventJsonAsRawString(SessionState sessionState, String eventJson) throws Exception {
        JsonNode hashableContent = OBJECT_MAPPER.readTree(sessionState.toHashableContent());
        JsonNode eventJsonNode = hashableContent.path("partialEvents").path(0).path("eventJson");

        assertThat(eventJsonNode.isTextual()).isTrue();
        assertThat(eventJsonNode.asText()).isEqualTo(eventJson);
    }
}
