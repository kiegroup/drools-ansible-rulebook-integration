package org.drools.ansible.rulebook.integration.ha.tests;

import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.crypto.KeyGenerator;

import org.drools.ansible.rulebook.integration.ha.api.HAEncryption;
import org.drools.ansible.rulebook.integration.ha.api.HAEncryptionException;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManager;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManagerFactory;
import org.drools.ansible.rulebook.integration.ha.api.HAUtils;
import org.drools.ansible.rulebook.integration.ha.model.EventRecord;
import org.drools.ansible.rulebook.integration.ha.model.MatchingEvent;
import org.drools.ansible.rulebook.integration.ha.model.SessionState;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.drools.ansible.rulebook.integration.ha.tests.TestUtils.createMatchingEvent;

/**
 * State manager integration tests for encryption.
 * Tests encryption/decryption through the actual database layer.
 */
class HAStateManagerEncryptionTest extends HAStateManagerTestBase {

    private HAStateManager stateManager;
    private String haUuid;
    private static final String LEADER_ID = "test-leader-1";
    private static final String RULE_SET_NAME = "encryptionTestRuleset";

    private static String generateBase64Key() {
        try {
            KeyGenerator keyGen = KeyGenerator.getInstance("AES");
            keyGen.init(256);
            return Base64.getEncoder().encodeToString(keyGen.generateKey().getEncoded());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Map<String, Object> configWithEncryption(String primaryKey, String secondaryKey) {
        Map<String, Object> config = new HashMap<>(dbHAConfig);
        config.put("encryption_key_primary", primaryKey);
        if (secondaryKey != null) {
            config.put("encryption_key_secondary", secondaryKey);
        }
        return config;
    }

    @BeforeEach
    void setUp() {
        haUuid = "test-enc-" + System.currentTimeMillis();
    }

    @AfterEach
    void tearDown() {
        if (stateManager != null) {
            stateManager.shutdown();
        }
        cleanupDatabase();
    }

    private HAStateManager createManager(Map<String, Object> config) {
        HAStateManager mgr = HAStateManagerFactory.create(TEST_DB_TYPE);
        mgr.initializeHA(haUuid, LEADER_ID, dbParams, config);
        mgr.enableLeader();
        return mgr;
    }

    @Test
    void persistAndLoadSessionStateWithEncryption() {
        String key = generateBase64Key();
        stateManager = createManager(configWithEncryption(key, null));

        // Create session state with partial events
        SessionState state = new SessionState();
        state.setHaUuid(haUuid);
        state.setRuleSetName(RULE_SET_NAME);
        state.setPartialEvents(List.of(
                new EventRecord("{\"temperature\": 35}", System.currentTimeMillis(), EventRecord.RecordType.EVENT)));
        state.setCreatedTime(System.currentTimeMillis());

        stateManager.persistSessionState(state);

        // Verify raw DB column is encrypted
        String rawPartialEvents = TestUtils.queryRawColumn(dbParams,
                "SELECT partial_matching_events FROM drools_ansible_session_state WHERE ha_uuid = ?", haUuid);
        assertThat(rawPartialEvents).startsWith("$ENCRYPTED$");
        assertThat(rawPartialEvents).doesNotContain("temperature");

        // Load and verify round trip
        SessionState loaded = stateManager.getPersistedSessionState(RULE_SET_NAME);
        assertThat(loaded).isNotNull();
        assertThat(loaded.getPartialEvents()).hasSize(1);
        assertThat(loaded.getPartialEvents().get(0).getEventJson()).isEqualTo("{\"temperature\": 35}");
    }

    @Test
    void persistAndLoadMatchingEventWithEncryption() {
        String key = generateBase64Key();
        stateManager = createManager(configWithEncryption(key, null));

        MatchingEvent me = createMatchingEvent(haUuid, RULE_SET_NAME, "temp_rule",
                Map.of("temperature", 35, "host", "server-01"));
        String meUuid = stateManager.addMatchingEvent(me);

        // Verify raw DB column is encrypted
        String rawEventData = TestUtils.queryRawColumn(dbParams,
                "SELECT event_data FROM drools_ansible_matching_event WHERE ha_uuid = ?", haUuid);
        assertThat(rawEventData).startsWith("$ENCRYPTED$");
        assertThat(rawEventData).doesNotContain("server-01");

        List<MatchingEvent> loaded = stateManager.getPendingMatchingEvents();
        assertThat(loaded).hasSize(1);
        assertThat(loaded.get(0).getMeUuid()).isEqualTo(meUuid);
        assertThat(loaded.get(0).getEventData()).contains("temperature");
        assertThat(loaded.get(0).getEventData()).contains("server-01");
    }

    @Test
    void persistAndLoadActionInfoWithEncryption() {
        String key = generateBase64Key();
        stateManager = createManager(configWithEncryption(key, null));

        MatchingEvent me = createMatchingEvent(haUuid, RULE_SET_NAME, "rule1", Map.of("key", "val"));
        String meUuid = stateManager.addMatchingEvent(me);

        String actionData = "{\"name\":\"run_playbook\",\"status\":0,\"url\":\"https://server/api\"}";
        stateManager.addActionInfo(meUuid, 0, actionData);

        // Verify raw DB column is encrypted
        String rawActionData = TestUtils.queryRawColumn(dbParams,
                "SELECT action_data FROM drools_ansible_action_info WHERE me_uuid = ?", meUuid);
        assertThat(rawActionData).startsWith("$ENCRYPTED$");
        assertThat(rawActionData).doesNotContain("run_playbook");

        String loaded = stateManager.getActionInfo(meUuid, 0);
        assertThat(loaded).contains("run_playbook");
        assertThat(loaded).contains("https://server/api");
    }

    @Test
    void keyRotationSessionState() {
        String keyA = generateBase64Key();
        String keyB = generateBase64Key();

        // Persist with keyA
        stateManager = createManager(configWithEncryption(keyA, null));
        SessionState state = new SessionState();
        state.setHaUuid(haUuid);
        state.setRuleSetName(RULE_SET_NAME);
        state.setPartialEvents(List.of(
                new EventRecord("{\"data\": \"secret\"}", System.currentTimeMillis(), EventRecord.RecordType.EVENT)));
        state.setCreatedTime(System.currentTimeMillis());
        stateManager.persistSessionState(state);
        stateManager.shutdown();

        // Load with keyB (primary) + keyA (secondary fallback)
        stateManager = createManager(configWithEncryption(keyB, keyA));
        SessionState loaded = stateManager.getPersistedSessionState(RULE_SET_NAME);
        assertThat(loaded).isNotNull();
        assertThat(loaded.getPartialEvents()).hasSize(1);
        assertThat(loaded.getPartialEvents().get(0).getEventJson()).isEqualTo("{\"data\": \"secret\"}");
    }

    @Test
    void keyRotationMatchingEvent() {
        String keyA = generateBase64Key();
        String keyB = generateBase64Key();

        // Persist with keyA
        stateManager = createManager(configWithEncryption(keyA, null));
        MatchingEvent me = createMatchingEvent(haUuid, RULE_SET_NAME, "rule1", Map.of("sensitive", "data"));
        stateManager.addMatchingEvent(me);
        stateManager.shutdown();

        // Load with keyB (primary) + keyA (secondary)
        stateManager = createManager(configWithEncryption(keyB, keyA));
        List<MatchingEvent> loaded = stateManager.getPendingMatchingEvents();
        assertThat(loaded).hasSize(1);
        assertThat(loaded.get(0).getEventData()).contains("sensitive");
    }

    @Test
    void wrongKeyFatalError() {
        String keyA = generateBase64Key();
        String keyB = generateBase64Key();

        // Persist with keyA
        stateManager = createManager(configWithEncryption(keyA, null));
        MatchingEvent me = createMatchingEvent(haUuid, RULE_SET_NAME, "rule1", Map.of("key", "val"));
        stateManager.addMatchingEvent(me);
        stateManager.shutdown();

        // Try to load with keyB only — should throw FATAL
        stateManager = createManager(configWithEncryption(keyB, null));
        assertThatThrownBy(() -> stateManager.getPendingMatchingEvents())
                .isInstanceOf(HAEncryptionException.class)
                .hasMessageContaining("FATAL");
    }

    @Test
    void noEncryptionBackwardCompatibility() {
        // No encryption keys — standard behavior
        stateManager = createManager(dbHAConfig);

        SessionState state = new SessionState();
        state.setHaUuid(haUuid);
        state.setRuleSetName(RULE_SET_NAME);
        state.setPartialEvents(List.of(
                new EventRecord("{\"temp\": 25}", System.currentTimeMillis(), EventRecord.RecordType.EVENT)));
        state.setCreatedTime(System.currentTimeMillis());
        stateManager.persistSessionState(state);

        SessionState loaded = stateManager.getPersistedSessionState(RULE_SET_NAME);
        assertThat(loaded).isNotNull();
        assertThat(loaded.getPartialEvents()).hasSize(1);
        assertThat(loaded.getPartialEvents().get(0).getEventJson()).isEqualTo("{\"temp\": 25}");
    }

    @Test
    void plaintextToEncryptionMigration() {
        // First: persist without encryption
        stateManager = createManager(dbHAConfig);
        SessionState state = new SessionState();
        state.setHaUuid(haUuid);
        state.setRuleSetName(RULE_SET_NAME);
        state.setPartialEvents(List.of(
                new EventRecord("{\"old\": \"data\"}", System.currentTimeMillis(), EventRecord.RecordType.EVENT)));
        state.setCreatedTime(System.currentTimeMillis());
        stateManager.persistSessionState(state);
        stateManager.shutdown();

        // Then: load with encryption enabled — old plaintext should pass through
        String key = generateBase64Key();
        stateManager = createManager(configWithEncryption(key, null));
        SessionState loaded = stateManager.getPersistedSessionState(RULE_SET_NAME);
        assertThat(loaded).isNotNull();
        assertThat(loaded.getPartialEvents()).hasSize(1);
        assertThat(loaded.getPartialEvents().get(0).getEventJson()).isEqualTo("{\"old\": \"data\"}");
    }

    @Test
    void startupWithEncryptedDataButNoKeys() {
        String key = generateBase64Key();

        // Persist with encryption
        stateManager = createManager(configWithEncryption(key, null));
        MatchingEvent me = createMatchingEvent(haUuid, RULE_SET_NAME, "rule1", Map.of("key", "val"));
        stateManager.addMatchingEvent(me);
        stateManager.shutdown();

        // Try to load WITHOUT encryption keys — startup guard should catch it
        stateManager = createManager(dbHAConfig);
        assertThatThrownBy(() -> stateManager.getPendingMatchingEvents())
                .isInstanceOf(HAEncryptionException.class)
                .hasMessageContaining("no encryption keys configured");
    }

    @Test
    void shaIntegrityWithEncryption() {
        String key = generateBase64Key();
        stateManager = createManager(configWithEncryption(key, null));

        SessionState state = new SessionState();
        state.setHaUuid(haUuid);
        state.setRuleSetName(RULE_SET_NAME);
        state.setPartialEvents(List.of(
                new EventRecord("{\"i\": 1}", System.currentTimeMillis(), EventRecord.RecordType.EVENT)));
        state.setCreatedTime(System.currentTimeMillis());

        // Calculate SHA on plaintext (as the real code does)
        String sha = HAUtils.calculateStateSHA(state);
        state.setCurrentStateSHA(sha);

        stateManager.persistSessionState(state);

        // Load — decryption happens, then SHA is verified on plaintext
        SessionState loaded = stateManager.getPersistedSessionState(RULE_SET_NAME);
        assertThat(loaded).isNotNull();
        assertThat(stateManager.verifySessionState(loaded)).isTrue();
    }

    @Test
    void updateActionInfoReEncryptsWithCurrentKey() {
        String key = generateBase64Key();
        stateManager = createManager(configWithEncryption(key, null));

        MatchingEvent me = createMatchingEvent(haUuid, RULE_SET_NAME, "rule1", Map.of("k", "v"));
        String meUuid = stateManager.addMatchingEvent(me);

        stateManager.addActionInfo(meUuid, 0, "{\"status\":0}");
        stateManager.updateActionInfo(meUuid, 0, "{\"status\":1}");

        String loaded = stateManager.getActionInfo(meUuid, 0);
        assertThat(loaded).contains("\"status\":1");
    }

    @Test
    void actionStatusWithEncryption() {
        // Tests the fetchActionStatusFromDatabase() code path
        String key = generateBase64Key();
        stateManager = createManager(configWithEncryption(key, null));

        MatchingEvent me = createMatchingEvent(haUuid, RULE_SET_NAME, "rule1", Map.of("k", "v"));
        String meUuid = stateManager.addMatchingEvent(me);

        stateManager.addActionInfo(meUuid, 0, "{\"name\":\"test\",\"status\":42}");

        String status = stateManager.getActionStatus(meUuid, 0);
        assertThat(status).isEqualTo("42");
    }
}
