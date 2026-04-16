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

class HAIntegrationDifferentHaUuidCoexistenceTest extends AbstractHATestBase {

    private static final String OLD_HA_UUID = "integration-old-ha";
    private static final String NEW_HA_UUID = "integration-new-ha";
    private static final String OLD_WORKER_NAME = "worker-old";
    private static final String NEW_WORKER_NAME = "worker-new";
    private static final String OLD_RULE_SET_NAME = "Legacy Coexistence Ruleset";
    private static final String NEW_RULE_SET_NAME = "New Coexistence Ruleset";

    private static final String OLD_RULE_SET = """
            {
                "name": "Legacy Coexistence Ruleset",
                "rules": [
                    {"Rule": {
                        "name": "legacy_temperature_alert",
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
                                    "name": "send_alert.yml"
                                }
                            ]
                        }
                    }}
                ]
            }
            """;

    private static final String NEW_RULE_SET = """
            {
                "name": "New Coexistence Ruleset",
                "rules": [
                    {"Rule": {
                        "name": "new_temperature_alert",
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
                                    "name": "send_alert.yml"
                                }
                            ]
                        }
                    }}
                ]
            }
            """;

    static {
        if (USE_POSTGRES) {
            initializePostgres("eda_ha_test", "HA different-ha_uuid coexistence integration tests");
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
    void newHaUuidRowBackedStateDoesNotTouchLegacyBlobStateOwnedByDifferentHaUuid() throws Exception {
        long now = System.currentTimeMillis();

        EventRecord oldRetainedEvent = new EventRecord("{\"meta\":{\"uuid\":\"old-legacy-event-1\"},\"i\":1}",
                                                       now,
                                                       EventRecord.RecordType.EVENT);
        SessionState oldLegacyState = new SessionState();
        oldLegacyState.setHaUuid(OLD_HA_UUID);
        oldLegacyState.setRuleSetName(OLD_RULE_SET_NAME);
        oldLegacyState.setRulebookHash(HAUtils.sha256(OLD_RULE_SET));
        oldLegacyState.setPartialEvents(List.of(oldRetainedEvent));
        oldLegacyState.setProcessedEventIds(List.of());
        oldLegacyState.setCreatedTime(now);
        oldLegacyState.setPersistedTime(now);
        oldLegacyState.setLeaderId(OLD_WORKER_NAME);
        oldLegacyState.setCurrentStateSHA(HAUtils.calculateStateSHA(oldLegacyState));

        createLegacySchemaWithoutEventRecordTable();
        insertLegacySessionState(oldLegacyState);

        rulesEngine = new AstRulesEngine();
        consumer = new AsyncConsumer("different-ha-uuid-consumer");
        consumer.startConsuming(rulesEngine.port());

        rulesEngine.initializeHA(NEW_HA_UUID, NEW_WORKER_NAME, dbParamsJson, dbHAConfigJson);

        assertThat(eventRecordTableExists()).isTrue();
        assertThat(TestUtils.queryRawColumn(dbParams,
                                            "SELECT COALESCE(partial_matching_events, '__NULL__') "
                                                    + "FROM drools_ansible_session_state WHERE ha_uuid = ?",
                                            OLD_HA_UUID)).isNotEqualTo("__NULL__");
        assertThat(TestUtils.queryRawColumn(dbParams,
                                            "SELECT COUNT(*) FROM drools_ansible_event_record WHERE ha_uuid = ?",
                                            OLD_HA_UUID)).isEqualTo("0");

        sessionId = rulesEngine.createRuleset(NEW_RULE_SET, RuleConfigurationOption.FULLY_MANUAL_PSEUDOCLOCK);
        rulesEngine.enableLeader();

        String result = rulesEngine.assertEvent(sessionId, createEvent("{\"i\":1}"));
        assertThat(readValueAsListOfMapOfStringAndObject(result)).isEmpty();

        assertThat(TestUtils.queryRawColumn(dbParams,
                                            "SELECT COALESCE(partial_matching_events, '__NULL__') "
                                                    + "FROM drools_ansible_session_state WHERE ha_uuid = ?",
                                            OLD_HA_UUID)).isNotEqualTo("__NULL__");
        assertThat(TestUtils.queryRawColumn(dbParams,
                                            "SELECT COUNT(*) FROM drools_ansible_event_record WHERE ha_uuid = ?",
                                            OLD_HA_UUID)).isEqualTo("0");

        assertThat(TestUtils.queryRawColumn(dbParams,
                                            "SELECT COALESCE(partial_matching_events, '__NULL__') "
                                                    + "FROM drools_ansible_session_state WHERE ha_uuid = ?",
                                            NEW_HA_UUID)).isEqualTo("__NULL__");
        assertThat(TestUtils.queryRawColumn(dbParams,
                                            "SELECT COUNT(*) FROM drools_ansible_event_record WHERE ha_uuid = ?",
                                            NEW_HA_UUID)).isEqualTo("1");

        HAStateManager oldManager = HAStateManagerFactory.create(TEST_DB_TYPE);
        try {
            oldManager.initializeHA(OLD_HA_UUID, "FOR_ASSERTION_OLD", dbParams, dbHAConfig);
            SessionState oldPersistedState = oldManager.getPersistedSessionState(OLD_RULE_SET_NAME);
            assertThat(oldPersistedState.getEventRecordsManifestSHA()).isNull();
            assertThat(oldPersistedState.getPartialEvents()).hasSize(1);
            assertThat(oldPersistedState.getPartialEvents().get(0).getEventJson()).contains("\"old-legacy-event-1\"");
            assertThat(oldManager.verifySessionState(oldPersistedState)).isTrue();
        } finally {
            oldManager.shutdown();
        }

        HAStateManager newManager = HAStateManagerFactory.create(TEST_DB_TYPE);
        try {
            newManager.initializeHA(NEW_HA_UUID, "FOR_ASSERTION_NEW", dbParams, dbHAConfig);
            SessionState newPersistedState = newManager.getPersistedSessionState(NEW_RULE_SET_NAME);
            assertThat(newPersistedState.getEventRecordsManifestSHA()).isNotNull();
            assertThat(newPersistedState.getPartialEvents()).hasSize(1);
            assertThat(newPersistedState.getPartialEvents().get(0).getEventJson()).contains("\"i\":1");
            assertThat(newManager.verifySessionState(newPersistedState)).isTrue();
        } finally {
            newManager.shutdown();
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
