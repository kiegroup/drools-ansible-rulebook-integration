package org.drools.ansible.rulebook.integration.ha.tests.integration.basic;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;

import org.drools.ansible.rulebook.integration.api.RuleConfigurationOption;
import org.drools.ansible.rulebook.integration.core.jpy.AstRulesEngine;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManager;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManagerFactory;
import org.drools.ansible.rulebook.integration.ha.api.HAUtils;
import org.drools.ansible.rulebook.integration.ha.model.EventRecord;
import org.drools.ansible.rulebook.integration.ha.model.SessionState;
import org.drools.ansible.rulebook.integration.ha.tests.integration.HAIntegrationTestBase.AsyncConsumer;
import org.drools.ansible.rulebook.integration.ha.tests.support.AbstractHATestBase;
import org.drools.ansible.rulebook.integration.ha.tests.support.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.readValueAsListOfMapOfStringAndObject;
import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.toJson;
import static org.drools.ansible.rulebook.integration.ha.tests.support.TestUtils.createEvent;

class HAIntegrationLegacyPartialEventsMigrationTest extends AbstractHATestBase {

    private static final String HA_UUID = "integration-legacy-migration";
    private static final String WORKER_NAME = "worker-legacy";
    private static final String RULE_SET_NAME = "Legacy Migration Ruleset";
    private static final String RULE_SET = """
            {
                "name": "Legacy Migration Ruleset",
                "rules": [
                    {"Rule": {
                        "name": "temperature_alert",
                        "condition": {
                            "AllCondition": [
                                {
                                    "EqualsExpression": {
                                        "lhs": {
                                            "Event": "i"
                                        },
                                        "rhs": {
                                            "Integer": 1
                                        }
                                    }
                                },
                                {
                                    "EqualsExpression": {
                                        "lhs": {
                                            "Event": "j"
                                        },
                                        "rhs": {
                                            "Integer": 2
                                        }
                                    }
                                }
                            ]
                        },
                        "action": {
                            "run_playbook": [
                                {
                                    "name": "send_alert.yml",
                                    "extra_vars": {
                                        "message": "High temperature detected"
                                    }
                                }
                            ]
                        }
                    }}
                ]
            }
            """;

    static {
        if (USE_POSTGRES) {
            initializePostgres("eda_ha_test", "HA legacy migration integration tests");
        } else {
            initializeH2();
        }
    }

    private AstRulesEngine rulesEngine;
    private AsyncConsumer consumer;
    private long sessionId;

    @AfterEach
    void tearDown() {
        if (consumer != null) {
            consumer.stop();
        }
        if (rulesEngine != null) {
            rulesEngine.dispose(sessionId);
            rulesEngine.close();
        }
        cleanupDatabase();
    }

    @Test
    void legacyPartialMatchingEventsMigratesToEventRecordRowsAfterStartupAndNextEvent() throws Exception {
        long now = System.currentTimeMillis();

        EventRecord retainedEvent = new EventRecord("{\"meta\":{\"uuid\":\"legacy-event-1\"},\"i\":1}",
                                                    now,
                                                    EventRecord.RecordType.EVENT);
        SessionState legacyState = new SessionState();
        legacyState.setHaUuid(HA_UUID);
        legacyState.setRuleSetName(RULE_SET_NAME);
        legacyState.setRulebookHash(HAUtils.sha256(RULE_SET));
        legacyState.setPartialEvents(List.of(retainedEvent));
        legacyState.setProcessedEventIds(List.of());
        legacyState.setCreatedTime(now);
        legacyState.setPersistedTime(now);
        legacyState.setLeaderId(WORKER_NAME);
        legacyState.setCurrentStateSHA(HAUtils.calculateStateSHA(legacyState));

        createLegacySchemaWithoutEventRecordTable();
        insertLegacySessionState(legacyState);

        assertThat(eventRecordTableExists()).isFalse();
        assertThat(TestUtils.queryRawColumn(dbParams,
                                            "SELECT COALESCE(partial_matching_events, '__NULL__') "
                                                    + "FROM drools_ansible_session_state WHERE ha_uuid = ?",
                                            HA_UUID)).isNotEqualTo("__NULL__");

        rulesEngine = new AstRulesEngine();
        consumer = new AsyncConsumer("legacy-migration-consumer");
        consumer.startConsuming(rulesEngine.port());

        rulesEngine.initializeHA(HA_UUID, WORKER_NAME, dbParamsJson, dbHAConfigJson);

        assertThat(eventRecordTableExists()).isTrue();
        assertThat(TestUtils.queryRawColumn(dbParams,
                                            "SELECT COALESCE(partial_matching_events, '__NULL__') "
                                                    + "FROM drools_ansible_session_state WHERE ha_uuid = ?",
                                            HA_UUID)).isNotEqualTo("__NULL__");
        assertThat(TestUtils.queryRawColumn(dbParams,
                                            "SELECT COUNT(*) FROM drools_ansible_event_record WHERE ha_uuid = ?",
                                            HA_UUID)).isEqualTo("0");

        sessionId = rulesEngine.createRuleset(RULE_SET, RuleConfigurationOption.FULLY_MANUAL_PSEUDOCLOCK);
        rulesEngine.enableLeader();

        String result = rulesEngine.assertEvent(sessionId, createEvent("{\"k\":3}"));
        assertThat(readValueAsListOfMapOfStringAndObject(result)).isEmpty();

        assertThat(TestUtils.queryRawColumn(dbParams,
                                            "SELECT COALESCE(partial_matching_events, '__NULL__') "
                                                    + "FROM drools_ansible_session_state WHERE ha_uuid = ?",
                                            HA_UUID)).isEqualTo("__NULL__");
        assertThat(TestUtils.queryRawColumn(dbParams,
                                            "SELECT COUNT(*) FROM drools_ansible_event_record WHERE ha_uuid = ?",
                                            HA_UUID)).isEqualTo("1");
        assertThat(TestUtils.queryRawColumn(dbParams,
                                            "SELECT event_json FROM drools_ansible_event_record WHERE ha_uuid = ?",
                                            HA_UUID)).contains("\"legacy-event-1\"")
                                                    .contains("\"i\":1");

        HAStateManager manager = HAStateManagerFactory.create(TEST_DB_TYPE);
        try {
            manager.initializeHA(HA_UUID, "FOR_ASSERTION", dbParams, dbHAConfig);
            SessionState persistedState = manager.getPersistedSessionState(RULE_SET_NAME);
            assertThat(persistedState.getEventRecordsManifestSHA()).isNotNull();
            assertThat(persistedState.getPartialEvents()).hasSize(1);
            assertThat(persistedState.getPartialEvents().get(0).getEventJson()).contains("\"legacy-event-1\"")
                                                                                 .contains("\"i\":1");
            assertThat(manager.verifySessionState(persistedState)).isTrue();
        } finally {
            manager.shutdown();
        }
    }

    private void createLegacySchemaWithoutEventRecordTable() throws SQLException {
        try (Connection conn = openConnection();
             PreparedStatement psSessionState = conn.prepareStatement(legacySessionStateDdl())) {
            psSessionState.execute();
        }
    }

    private void insertLegacySessionState(SessionState legacyState) throws SQLException {
        String sql = "INSERT INTO drools_ansible_session_state "
                + "(ha_uuid, rule_set_name, rulebook_hash, partial_matching_events, processed_event_ids, persisted_time, current_state_sha, created_time, leader_id) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (Connection conn = openConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, legacyState.getHaUuid());
            ps.setString(2, legacyState.getRuleSetName());
            ps.setString(3, legacyState.getRulebookHash());
            ps.setString(4, toJson(legacyState.getPartialEvents()));
            ps.setString(5, toJson(legacyState.getProcessedEventIds()));
            ps.setTimestamp(6, new Timestamp(legacyState.getPersistedTime()));
            ps.setString(7, legacyState.getCurrentStateSHA());
            ps.setTimestamp(8, new Timestamp(legacyState.getCreatedTime()));
            ps.setString(9, legacyState.getLeaderId());
            ps.executeUpdate();
        }
    }

    private boolean eventRecordTableExists() throws SQLException {
        String sql = USE_POSTGRES
                ? "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'drools_ansible_event_record')"
                : "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'DROOLS_ANSIBLE_EVENT_RECORD'";
        try (Connection conn = openConnection();
             PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            rs.next();
            return USE_POSTGRES ? rs.getBoolean(1) : rs.getInt(1) > 0;
        }
    }

    private Connection openConnection() throws SQLException {
        if (USE_POSTGRES) {
            String jdbcUrl = String.format("jdbc:postgresql://%s:%d/%s",
                                           dbParams.get("host"),
                                           dbParams.get("port"),
                                           dbParams.get("database"));
            return DriverManager.getConnection(jdbcUrl,
                                               (String) dbParams.get("user"),
                                               (String) dbParams.get("password"));
        }
        String jdbcUrl = "jdbc:h2:file:" + dbParams.get("db_file_path") + ";MODE=PostgreSQL";
        return DriverManager.getConnection(jdbcUrl, "sa", "");
    }

    private String legacySessionStateDdl() {
        if (USE_POSTGRES) {
            return "CREATE TABLE drools_ansible_session_state ("
                    + "id BIGSERIAL PRIMARY KEY, "
                    + "ha_uuid VARCHAR(255) NOT NULL, "
                    + "rule_set_name VARCHAR(255) NOT NULL, "
                    + "rulebook_hash VARCHAR(64) NOT NULL, "
                    + "partial_matching_events TEXT, "
                    + "processed_event_ids TEXT, "
                    + "persisted_time TIMESTAMP, "
                    + "current_state_sha VARCHAR(64) NOT NULL, "
                    + "created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP, "
                    + "leader_id VARCHAR(255), "
                    + "UNIQUE(ha_uuid, rule_set_name)"
                    + ")";
        }
        return "CREATE TABLE drools_ansible_session_state ("
                + "id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, "
                + "ha_uuid VARCHAR(255) NOT NULL, "
                + "rule_set_name VARCHAR(255) NOT NULL, "
                + "rulebook_hash VARCHAR(64) NOT NULL, "
                + "partial_matching_events CLOB, "
                + "processed_event_ids CLOB, "
                + "persisted_time TIMESTAMP, "
                + "current_state_sha VARCHAR(64) NOT NULL, "
                + "created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP, "
                + "leader_id VARCHAR(255), "
                + "UNIQUE(ha_uuid, rule_set_name)"
                + ")";
    }
}
