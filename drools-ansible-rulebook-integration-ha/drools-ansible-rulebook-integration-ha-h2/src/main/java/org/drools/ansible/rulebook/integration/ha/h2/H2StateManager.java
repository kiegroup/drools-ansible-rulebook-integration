package org.drools.ansible.rulebook.integration.ha.h2;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.drools.ansible.rulebook.integration.ha.api.AbstractHAStateManager;
import org.drools.ansible.rulebook.integration.ha.model.SessionState;
import org.drools.ansible.rulebook.integration.ha.model.HAStats;
import org.drools.ansible.rulebook.integration.ha.model.MatchingEvent;
import org.drools.ansible.rulebook.integration.ha.model.EventRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.readValueAsMapOfStringAndObject;
import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.readValue;
import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.toJson;
import static org.drools.ansible.rulebook.integration.ha.api.HATableNames.ACTION_INFO;
import static org.drools.ansible.rulebook.integration.ha.api.HATableNames.HA_STATS;
import static org.drools.ansible.rulebook.integration.ha.api.HATableNames.MATCHING_EVENT;
import static org.drools.ansible.rulebook.integration.ha.api.HATableNames.SESSION_STATE;

/**
 * H2 implementation of HAStateManager with simplified domain model
 */
public class H2StateManager extends AbstractHAStateManager {

    private static final Logger logger = LoggerFactory.getLogger(H2StateManager.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<List<EventRecord>> EVENT_RECORD_LIST_TYPE = new TypeReference<>() {};
    private static final TypeReference<List<String>> STRING_LIST_TYPE = new TypeReference<>() {};

    @FunctionalInterface
    private interface SqlWork {
        void execute(Connection conn) throws SQLException;
    }

    private HikariDataSource dataSource;
    private String leaderId;
    private boolean isLeader = false;
    private String haUuid;
    private String workerName;
    private HAStats haStats;

    public H2StateManager() {
    }

    // For debugging purposes
    public void printDatabaseContents() {
        try (var conn = dataSource.getConnection();
             var stmt = conn.createStatement()) {
            try (var rs = stmt.executeQuery("SELECT ha_uuid, properties FROM " + HA_STATS)) {
                while (rs.next()) {
                    logger.info("#### HAStats row: ha_uuid=" + rs.getString("ha_uuid")
                                               + ", properties=" + rs.getString("properties"));
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // ── Transaction helper ──────────────────────────────────────────────

    private void executeInTransaction(String errorMessage, SqlWork work) {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try {
                work.execute(conn);
                conn.commit();
            } catch (Exception e) {
                conn.rollback();
                throw e;
            }
        } catch (SQLException e) {
            logger.error(errorMessage, e);
            throw new RuntimeException(errorMessage, e);
        }
    }

    // ── Initialization ──────────────────────────────────────────────────

    @Override
    public void initializeHA(String uuid, String workerName, Map<String, Object> dbParams, Map<String, Object> config) {
        logger.info("Initializing HA mode with UUID: {}, workerName: {}", uuid, workerName);

        this.haUuid = uuid;
        this.workerName = workerName;
        this.haStats = new HAStats(uuid);

        this.dataSource = buildDataSource(dbParams);

        try {
            H2Schema.createSchema(dataSource);
            H2Schema.migrateSchema(dataSource);

            loadOrCreateHAStats();
        } catch (SQLException e) {
            logger.error("Failed to initialize HA schema", e);
            throw new RuntimeException("Failed to initialize HA schema", e);
        }

        commonInit(config);
    }

    private HikariDataSource buildDataSource(Map<String, Object> dbParams) {
        String dbFilePath = dbParams != null ? (String) dbParams.get("db_file_path") : null;
        if (dbFilePath == null || dbFilePath.isEmpty()) {
            dbFilePath = "./eda_ha";
        }
        String jdbcUrl = "jdbc:h2:file:" + dbFilePath + ";MODE=PostgreSQL;FILE_LOCK=NO";
        logger.info("Using file-backed H2 database from db_file_path: {}", dbFilePath);
        logger.warn("Using H2 database for HA - not suitable for production");

        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(jdbcUrl);
        hikariConfig.setUsername("sa");
        hikariConfig.setPassword("");
        hikariConfig.setMaximumPoolSize(10);
        hikariConfig.setDriverClassName("org.h2.Driver");

        return new HikariDataSource(hikariConfig);
    }

    // ── Leader management ───────────────────────────────────────────────

    @Override
    public void enableLeader() {
        this.leaderId = this.workerName;
        this.isLeader = true;

        String selectSql = "SELECT * FROM " + HA_STATS + " WHERE ha_uuid = ?";

        // Read HAStats from DB and mutate in-memory only — do NOT persist yet.
        // HAStats will be persisted later in persistLeaderStartup().
        executeInTransaction("Failed to enable leader", conn -> {
            try (PreparedStatement ps = conn.prepareStatement(selectSql)) {
                ps.setString(1, haUuid);
                ResultSet rs = ps.executeQuery();
                if (rs.next()) {
                    populateHAStatsFromResultSet(rs);
                    logger.info("Restored HA stats from database");
                }
            }

            haStats.setCurrentLeader(this.workerName);
            if (haStats.getHaUuid() == null) {
                haStats.setHaUuid(haUuid);
            }
            ensureVersionInMetadata(haStats.getMetadata());
            // No doHAStatsUpsert(conn) — deferred to persistLeaderStartup()
        });

        logger.info("Leader mode enabled for: {}", this.workerName);
    }

    @Override
    public void disableLeader() {
        if (leaderId != null && leaderId.equals(this.workerName)) {
            this.isLeader = false;
            this.leaderId = null;
        }

        logger.info("Leader mode disabled for: {}", this.workerName);
    }

    @Override
    public boolean isLeader() {
        return isLeader;
    }

    @Override
    public String getHaUuid() {
        return haUuid;
    }

    @Override
    public String getLeaderId() {
        return leaderId;
    }

    @Override
    public String getWorkerName() {
        return workerName;
    }

    // ── SessionState operations ─────────────────────────────────────────

    @Override
    public SessionState getPersistedSessionState(String ruleSetName) {
        String sql = "SELECT * FROM " + SESSION_STATE
                + " WHERE ha_uuid = ? AND rule_set_name = ?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, haUuid);
            ps.setString(2, ruleSetName);
            ResultSet rs = ps.executeQuery();

            if (rs.next()) {
                SessionState sessionState = new SessionState();
                sessionState.setHaUuid(rs.getString("ha_uuid"));
                sessionState.setRuleSetName(rs.getString("rule_set_name"));
                sessionState.setRulebookHash(rs.getString("rulebook_hash"));

                String partialEventsJson = decryptIfEnabled(rs.getString("partial_matching_events"));
                if (partialEventsJson != null) {
                    try {
                        List<EventRecord> partialEvents = OBJECT_MAPPER.readValue(partialEventsJson, EVENT_RECORD_LIST_TYPE);
                        sessionState.setPartialEvents(partialEvents);
                    } catch (Exception e) {
                        logger.error("Failed to deserialize partial events", e);
                    }
                }

                String processedEventIdsJson = rs.getString("processed_event_ids");
                if (processedEventIdsJson != null) {
                    try {
                        List<String> processedEventIds = OBJECT_MAPPER.readValue(processedEventIdsJson, STRING_LIST_TYPE);
                        sessionState.setProcessedEventIds(processedEventIds);
                    } catch (Exception e) {
                        logger.error("Failed to deserialize processed event IDs", e);
                    }
                }

                Timestamp persistedTime = rs.getTimestamp("persisted_time");
                if (persistedTime != null) {
                    sessionState.setPersistedTime(persistedTime.getTime());
                }

                sessionState.setLeaderId(rs.getString("leader_id"));
                sessionState.setCurrentStateSHA(rs.getString("current_state_sha"));

                Timestamp createdTime = rs.getTimestamp("created_time");
                if (createdTime != null) {
                    sessionState.setCreatedTime(createdTime.getTime());
                }

                sessionState.setMetadata(jsonToMap(rs.getString("metadata")));
                sessionState.setProperties(jsonToMap(rs.getString("properties")));
                sessionState.setSettings(jsonToMap(rs.getString("settings")));
                sessionState.setExt(jsonToMap(rs.getString("ext")));

                logger.info("Loaded SessionState from database: {}", ruleSetName);

                return sessionState;
            }
        } catch (SQLException e) {
            logger.error("Failed to get SessionState", e);
        }

        return null;
    }

    @Override
    public void persistSessionState(SessionState sessionState) {
        validateForPersist(sessionState);
        ensureVersionInMetadata(sessionState.getMetadata());

        executeInTransaction("Failed to persist SessionState", conn -> {
            doSessionStateUpsert(conn, sessionState);
        });

        logger.debug("Persisted SessionState for haUuid: {}", haUuid);
    }

    @Override
    public void persistSessionStateAndStats(SessionState sessionState) {
        validateForPersist(sessionState);

        if (haStats == null) {
            // Defensive: haStats should always be initialized before this is called on the hot path
            persistSessionState(sessionState);
            return;
        }

        ensureVersionInMetadata(sessionState.getMetadata());
        prepareHAStatsForPersist();

        executeInTransaction("Failed to persist SessionState and HAStats", conn -> {
            doSessionStateUpsert(conn, sessionState);
            doHAStatsUpsert(conn);
        });

        logger.debug("Persisted SessionState and HAStats in single transaction for haUuid: {}", haUuid);
    }

    @Override
    public void persistSessionStateStatsAndMatchingEvents(SessionState sessionState, List<MatchingEvent> matchingEvents) {
        validateForPersist(sessionState);

        if (haStats == null) {
            // Defensive: haStats should always be initialized before this is called on the hot path
            persistSessionState(sessionState);
            for (MatchingEvent me : matchingEvents) {
                addMatchingEvent(me);
            }
            return;
        }

        ensureVersionInMetadata(sessionState.getMetadata());
        prepareHAStatsForPersist();

        // Pre-compute encrypted data outside the transaction to minimize lock duration
        String encryptedPartialEvents = null;
        if (sessionState.getPartialEvents() != null) {
            encryptedPartialEvents = encryptIfEnabled(toJson(sessionState.getPartialEvents()));
        }

        List<String> encryptedEventDataList = new ArrayList<>();
        if (!matchingEvents.isEmpty()) {
            for (MatchingEvent me : matchingEvents) {
                ensureVersionInMetadata(me.getMetadata());
                encryptedEventDataList.add(encryptIfEnabled(me.getEventData()));
            }
        }

        final String finalEncryptedPartialEvents = encryptedPartialEvents;
        executeInTransaction("Failed to persist SessionState, HAStats, and matching events", conn -> {
            doSessionStateUpsert(conn, sessionState, finalEncryptedPartialEvents);
            doHAStatsUpsert(conn);

            for (int i = 0; i < matchingEvents.size(); i++) {
                doMatchingEventInsert(conn, matchingEvents.get(i), encryptedEventDataList.get(i));
            }
        });

        logger.debug("Persisted SessionState, HAStats, and {} matching events in single transaction for haUuid: {}",
                     matchingEvents.size(), haUuid);
    }

    private void validateForPersist(SessionState sessionState) {
        if (!isLeader) {
            throw new IllegalStateException("Cannot persist SessionState - not leader");
        }
        if (sessionState.getRuleSetName() == null) {
            throw new IllegalArgumentException("SessionState.ruleSetName must be set");
        }
    }

    private void prepareHAStatsForPersist() {
        if (haStats.getHaUuid() == null) {
            haStats.setHaUuid(haUuid);
        }
        ensureVersionInMetadata(haStats.getMetadata());
    }

    private void doSessionStateUpsert(Connection conn, SessionState sessionState) throws SQLException {
        String encryptedPartialEvents = null;
        if (sessionState.getPartialEvents() != null) {
            encryptedPartialEvents = encryptIfEnabled(toJson(sessionState.getPartialEvents()));
        }
        doSessionStateUpsert(conn, sessionState, encryptedPartialEvents);
    }

    private void doSessionStateUpsert(Connection conn, SessionState sessionState, String encryptedPartialEvents) throws SQLException {
        String sql = "MERGE INTO " + SESSION_STATE
                + " (ha_uuid, rule_set_name, rulebook_hash, partial_matching_events, processed_event_ids, persisted_time, current_state_sha,"
                + " created_time, leader_id, metadata, properties, settings, ext)"
                + " KEY(ha_uuid, rule_set_name)"
                + " VALUES (?, ?, ?, ?, ?, ?, ?,"
                + " COALESCE((SELECT created_time FROM " + SESSION_STATE + " WHERE ha_uuid = ? AND rule_set_name = ?), ?),"
                + " ?, ?, ?, ?, ?)";

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, sessionState.getHaUuid());
            ps.setString(2, sessionState.getRuleSetName());
            ps.setString(3, sessionState.getRulebookHash());
            ps.setString(4, encryptedPartialEvents);

            String processedEventIdsJson = null;
            if (sessionState.getProcessedEventIds() != null) {
                processedEventIdsJson = toJson(sessionState.getProcessedEventIds());
            }
            ps.setString(5, processedEventIdsJson);

            if (sessionState.getPersistedTime() > 0) {
                ps.setTimestamp(6, new Timestamp(sessionState.getPersistedTime()));
            } else {
                ps.setTimestamp(6, null);
            }

            ps.setString(7, sessionState.getCurrentStateSHA());

            // Subquery params for COALESCE(created_time) — preserves original on update
            ps.setString(8, sessionState.getHaUuid());
            ps.setString(9, sessionState.getRuleSetName());

            // Fallback created_time for first insert
            if (sessionState.getCreatedTime() > 0) {
                ps.setTimestamp(10, new Timestamp(sessionState.getCreatedTime()));
            } else {
                ps.setTimestamp(10, new Timestamp(System.currentTimeMillis()));
            }

            ps.setString(11, sessionState.getLeaderId());

            ps.setString(12, mapToJson(sessionState.getMetadata()));
            ps.setString(13, mapToJson(sessionState.getProperties()));
            ps.setString(14, mapToJson(sessionState.getSettings()));
            ps.setString(15, mapToJson(sessionState.getExt()));

            ps.executeUpdate();
        }
    }

    // ── MatchingEvent operations ────────────────────────────────────────

    @Override
    public String addMatchingEvent(MatchingEvent matchingEvent) {
        if (!isLeader) {
            throw new IllegalStateException("Cannot add matching event - not leader");
        }

        String meUuid = UUID.randomUUID().toString();
        matchingEvent.setMeUuid(meUuid);
        if (matchingEvent.getCreatedAt() == 0L) {
            matchingEvent.setCreatedAt(System.currentTimeMillis());
        }

        ensureVersionInMetadata(matchingEvent.getMetadata());

        String encryptedEventData = encryptIfEnabled(matchingEvent.getEventData());

        executeInTransaction("Failed to add matching event", conn -> {
            doMatchingEventInsert(conn, matchingEvent, encryptedEventData);
        });

        logger.debug("Added matching event with UUID: {} for rule: {}/{}",
                     meUuid, matchingEvent.getRuleSetName(), matchingEvent.getRuleName());

        return meUuid;
    }

    @Override
    public List<String> addMatchingEvents(List<MatchingEvent> matchingEvents) {
        if (!isLeader) {
            throw new IllegalStateException("Cannot add matching events - not leader");
        }
        if (matchingEvents.isEmpty()) {
            return List.of();
        }

        List<String> meUuids = new ArrayList<>();
        List<String> encryptedEventDataList = new ArrayList<>();
        for (MatchingEvent me : matchingEvents) {
            String meUuid = UUID.randomUUID().toString();
            meUuids.add(meUuid);
            me.setMeUuid(meUuid);
            if (me.getCreatedAt() == 0L) {
                me.setCreatedAt(System.currentTimeMillis());
            }
            ensureVersionInMetadata(me.getMetadata());
            encryptedEventDataList.add(encryptIfEnabled(me.getEventData()));
        }

        executeInTransaction("Failed to add matching events", conn -> {
            for (int i = 0; i < matchingEvents.size(); i++) {
                doMatchingEventInsert(conn, matchingEvents.get(i), encryptedEventDataList.get(i));
            }
        });

        for (String meUuid : meUuids) {
            logger.debug("Added matching event with UUID: {}", meUuid);
        }
        return meUuids;
    }

    @Override
    public List<MatchingEvent> getPendingMatchingEvents() {
        String sql = "SELECT * FROM " + MATCHING_EVENT + " WHERE ha_uuid = ? ORDER BY created_at";
        List<MatchingEvent> events = new ArrayList<>();

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, haUuid);
            ResultSet rs = ps.executeQuery();

            while (rs.next()) {
                MatchingEvent event = new MatchingEvent();
                event.setMeUuid(rs.getString("me_uuid"));
                event.setHaUuid(rs.getString("ha_uuid"));
                event.setRuleSetName(rs.getString("rule_set_name"));
                event.setRuleName(rs.getString("rule_name"));
                event.setEventData(decryptIfEnabled(rs.getString("event_data")));
                event.setCreatedAt(rs.getLong("created_at"));
                event.setMetadata(jsonToMap(rs.getString("metadata")));
                event.setProperties(jsonToMap(rs.getString("properties")));
                event.setSettings(jsonToMap(rs.getString("settings")));
                event.setExt(jsonToMap(rs.getString("ext")));
                events.add(event);
            }
        } catch (SQLException e) {
            logger.error("Failed to get pending matching events", e);
        }

        return events;
    }

    private void doMatchingEventInsert(Connection conn, MatchingEvent matchingEvent, String encryptedEventData) throws SQLException {
        String sql = "INSERT INTO " + MATCHING_EVENT
                + " (me_uuid, ha_uuid, rule_set_name, rule_name, event_data, created_at,"
                + " metadata, properties, settings, ext)"
                + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, matchingEvent.getMeUuid());
            ps.setString(2, matchingEvent.getHaUuid());
            ps.setString(3, matchingEvent.getRuleSetName());
            ps.setString(4, matchingEvent.getRuleName());
            ps.setString(5, encryptedEventData);
            ps.setLong(6, matchingEvent.getCreatedAt());
            ps.setString(7, mapToJson(matchingEvent.getMetadata()));
            ps.setString(8, mapToJson(matchingEvent.getProperties()));
            ps.setString(9, mapToJson(matchingEvent.getSettings()));
            ps.setString(10, mapToJson(matchingEvent.getExt()));

            ps.executeUpdate();
        }
    }

    // ── ActionInfo operations ───────────────────────────────────────────

    @Override
    public void addActionInfo(String matchingUuid, int index, String action) {
        if (!isLeader) {
            throw new IllegalStateException("Cannot add action - not leader");
        }

        String actionId = UUID.randomUUID().toString();

        String encryptedAction = encryptIfEnabled(action);
        String metadataJson = mapToJson(Map.of(DROOLS_VERSION_KEY, DROOLS_VERSION));

        String sql = "INSERT INTO " + ACTION_INFO
                + " (id, ha_uuid, me_uuid, index, action_data,"
                + " metadata, properties, settings, ext)"
                + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

        executeInTransaction("Failed to add action", conn -> {
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, actionId);
                ps.setString(2, haUuid);
                ps.setString(3, matchingUuid);
                ps.setInt(4, index);
                ps.setString(5, encryptedAction);
                ps.setString(6, metadataJson);
                ps.setString(7, "{}");
                ps.setString(8, "{}");
                ps.setString(9, "{}");

                ps.executeUpdate();
            }

            if (haStats != null) {
                haStats.incrementActionsProcessed();
                ensureVersionInMetadata(haStats.getMetadata());
                doHAStatsUpsert(conn);
            }
        });

        logger.debug("Added action for ME UUID: {}, index: {}", matchingUuid, index);
    }

    @Override
    public void updateActionInfo(String matchingUuid, int index, String action) {
        if (!isLeader) {
            logger.debug("Not leader - skipping action update");
            return;
        }

        String sql = "UPDATE " + ACTION_INFO + " SET action_data = ? WHERE me_uuid = ? AND index = ?";

        executeInTransaction("Failed to update action", conn -> {
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, encryptIfEnabled(action));
                ps.setString(2, matchingUuid);
                ps.setInt(3, index);

                int updated = ps.executeUpdate();

                if (updated > 0) {
                    logger.debug("Updated action for ME UUID: {}, index: {}", matchingUuid, index);
                } else {
                    logger.warn("No action found to update for ME UUID: {}, index: {}", matchingUuid, index);
                }
            }
        });
    }

    @Override
    public String getActionStatus(String matchingUuid, int index) {
        Integer status = fetchActionStatusFromDatabase(matchingUuid, index);
        return status == null ? "" : Integer.toString(status);
    }

    @Override
    public boolean actionInfoExists(String matchingUuid, int index) {
        String sql = "SELECT COUNT(*) FROM " + ACTION_INFO + " WHERE me_uuid = ? AND index = ?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, matchingUuid);
            ps.setInt(2, index);

            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return rs.getInt(1) > 0;
            }
        } catch (SQLException e) {
            logger.error("Failed to check action existence", e);
        }

        return false;
    }

    @Override
    public String getActionInfo(String matchingUuid, int index) {
        String sql = "SELECT action_data FROM " + ACTION_INFO + " WHERE me_uuid = ? AND index = ?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, matchingUuid);
            ps.setInt(2, index);

            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                String actionData = decryptIfEnabled(rs.getString("action_data"));
                if (actionData != null) {
                    return actionData;
                }
            }
        } catch (SQLException e) {
            logger.error("Failed to get action", e);
        }

        return "";
    }

    @Override
    public void deleteActionInfo(String matchingUuid) {
        if (!isLeader) {
            logger.debug("Not leader - skipping action deletion");
            return;
        }

        executeInTransaction("Failed to delete actions", conn -> {
            String sqlActions = "DELETE FROM " + ACTION_INFO + " WHERE me_uuid = ?";
            try (PreparedStatement ps1 = conn.prepareStatement(sqlActions)) {
                ps1.setString(1, matchingUuid);
                ps1.executeUpdate();
            }

            String sqlME = "DELETE FROM " + MATCHING_EVENT + " WHERE me_uuid = ?";
            try (PreparedStatement ps2 = conn.prepareStatement(sqlME)) {
                ps2.setString(1, matchingUuid);
                ps2.executeUpdate();
            }
        });

        logger.debug("Deleted matching event and actions: {}", matchingUuid);
    }

    @Override
    public void deleteSessionState(String ruleSetName) {
        if (!isLeader) {
            throw new IllegalStateException("Cannot delete session state - not leader");
        }

        executeInTransaction("Failed to delete SessionState", conn -> {
            String sql = "DELETE FROM " + SESSION_STATE + " WHERE ha_uuid = ? AND rule_set_name = ?";
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, haUuid);
                ps.setString(2, ruleSetName);
                ps.executeUpdate();
            }
        });

        logger.debug("Deleted SessionState for ruleSetName: {}", ruleSetName);
    }

    @Override
    public void persistSessionStateAndMatchingEvents(SessionState sessionState, List<MatchingEvent> matchingEvents) {
        validateForPersist(sessionState);
        ensureVersionInMetadata(sessionState.getMetadata());

        List<String> encryptedEventDataList = new ArrayList<>();
        if (matchingEvents != null) {
            for (MatchingEvent me : matchingEvents) {
                if (me.getMeUuid() == null) {
                    me.setMeUuid(UUID.randomUUID().toString());
                }
                if (me.getCreatedAt() == 0L) {
                    me.setCreatedAt(System.currentTimeMillis());
                }
                ensureVersionInMetadata(me.getMetadata());
                encryptedEventDataList.add(encryptIfEnabled(me.getEventData()));
            }
        }

        executeInTransaction("Failed to persist SessionState and matching events", conn -> {
            doSessionStateUpsert(conn, sessionState);
            if (matchingEvents != null) {
                for (int i = 0; i < matchingEvents.size(); i++) {
                    doMatchingEventInsert(conn, matchingEvents.get(i), encryptedEventDataList.get(i));
                }
            }
        });

        logger.debug("Persisted SessionState and {} matching events in single transaction for haUuid: {}",
                     matchingEvents != null ? matchingEvents.size() : 0, haUuid);
    }

    @Override
    public void persistLeaderStartup(List<SessionState> sessionStatesToPersist,
                                     List<String> rulesetNamesToDelete,
                                     List<MatchingEvent> matchingEvents) {
        // Pre-transaction: validate, encrypt, assign UUIDs
        for (SessionState ss : sessionStatesToPersist) {
            validateForPersist(ss);
            ensureVersionInMetadata(ss.getMetadata());
        }

        List<String> encryptedEventDataList = new ArrayList<>();
        if (matchingEvents != null) {
            for (MatchingEvent me : matchingEvents) {
                if (me.getMeUuid() == null) {
                    me.setMeUuid(UUID.randomUUID().toString());
                }
                if (me.getCreatedAt() == 0L) {
                    me.setCreatedAt(System.currentTimeMillis());
                }
                ensureVersionInMetadata(me.getMetadata());
                encryptedEventDataList.add(encryptIfEnabled(me.getEventData()));
            }
        }

        ensureVersionInMetadata(haStats.getMetadata());

        executeInTransaction("Failed to persist leader startup", conn -> {
            // 1. Upsert HAStats
            doHAStatsUpsert(conn);

            // 2. Delete old session states (hash-mismatch rulesets)
            if (rulesetNamesToDelete != null) {
                String deleteSql = "DELETE FROM " + SESSION_STATE + " WHERE ha_uuid = ? AND rule_set_name = ?";
                for (String rulesetName : rulesetNamesToDelete) {
                    try (PreparedStatement ps = conn.prepareStatement(deleteSql)) {
                        ps.setString(1, haUuid);
                        ps.setString(2, rulesetName);
                        ps.executeUpdate();
                    }
                }
            }

            // 3. Upsert all refreshed session states
            for (SessionState ss : sessionStatesToPersist) {
                doSessionStateUpsert(conn, ss);
            }

            // 4. Insert all recovery matching events
            if (matchingEvents != null) {
                for (int i = 0; i < matchingEvents.size(); i++) {
                    doMatchingEventInsert(conn, matchingEvents.get(i), encryptedEventDataList.get(i));
                }
            }
        });

        logger.info("Persisted leader startup in single transaction: {} session states, {} deletes, {} matching events",
                     sessionStatesToPersist.size(),
                     rulesetNamesToDelete != null ? rulesetNamesToDelete.size() : 0,
                     matchingEvents != null ? matchingEvents.size() : 0);
    }

    // ── HAStats operations ──────────────────────────────────────────────

    @Override
    public HAStats getHAStats() {
        if (haStats != null) {
            haStats.setPartialEventsInMemory(countPartialEventsInMemory());
        }
        return haStats;
    }

    @Override
    public void refreshHAStats() {
        if (haStats != null) {
            haStats.setIncompleteMatchingEvents(countRows("SELECT COUNT(*) AS cnt FROM " + MATCHING_EVENT + " WHERE ha_uuid = ?"));
            haStats.setSessionStateSize(calculateSessionStateSize());
        }
    }

    public HAStats loadOrCreateHAStats() {
        String sql = "SELECT * FROM " + HA_STATS + " WHERE ha_uuid = ?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, haUuid);
            ResultSet rs = ps.executeQuery();

            if (rs.next()) {
                populateHAStatsFromResultSet(rs);
                logger.info("Restored HA stats from database");
            } else {
                persistHAStats();
            }
        } catch (SQLException e) {
            logger.error("Failed to load HA stats", e);
            throw new RuntimeException("Failed to load HA stats", e);
        }
        return haStats;
    }

    @Override
    public void persistHAStats() {
        if (haStats == null) {
            return;
        }

        prepareHAStatsForPersist();

        executeInTransaction("Failed to persist HA stats", conn -> {
            doHAStatsUpsert(conn);
        });
    }

    private void populateHAStatsFromResultSet(ResultSet rs) throws SQLException {
        haStats.setHaUuid(rs.getString("ha_uuid"));
        populateHAStatsFromJson(rs.getString("properties"), haStats);
        haStats.setMetadata(jsonToMap(rs.getString("metadata")));
        haStats.setSettings(jsonToMap(rs.getString("settings")));
        haStats.setExt(jsonToMap(rs.getString("ext")));
    }

    private Long calculateSessionStateSize() {
        try (Connection conn = dataSource.getConnection()) {
            return doCalculateSessionStateSize(conn);
        } catch (SQLException e) {
            logger.warn("Failed to calculate session state size: {}", e.getMessage());
        }
        return 0L;
    }

    private Long doCalculateSessionStateSize(Connection conn) throws SQLException {
        String sql = "SELECT"
                + " OCTET_LENGTH(ha_uuid) +"
                + " OCTET_LENGTH(rule_set_name) +"
                + " COALESCE(OCTET_LENGTH(rulebook_hash), 0) +"
                + " COALESCE(OCTET_LENGTH(partial_matching_events), 0) +"
                + " COALESCE(OCTET_LENGTH(metadata), 0) +"
                + " COALESCE(OCTET_LENGTH(properties), 0) +"
                + " COALESCE(OCTET_LENGTH(settings), 0) +"
                + " COALESCE(OCTET_LENGTH(ext), 0) +"
                + " 8 + 8 + 8 + 8 AS total_size"
                + " FROM " + SESSION_STATE
                + " WHERE ha_uuid = ?";

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, haUuid);
            ResultSet rs = ps.executeQuery();

            if (rs.next()) {
                return rs.getLong("total_size");
            }
        }
        return 0L;
    }

    private void doHAStatsUpsert(Connection conn) throws SQLException {
        String propertiesJson = haStatsToJson(haStats);

        String h2Sql = "MERGE INTO " + HA_STATS
                + " (ha_uuid, properties, updated_at, metadata, settings, ext)"
                + " KEY(ha_uuid) VALUES (?, ?, ?, ?, ?, ?)";

        try (PreparedStatement ps = conn.prepareStatement(h2Sql)) {
            ps.setString(1, haStats.getHaUuid());
            ps.setString(2, propertiesJson);
            ps.setTimestamp(3, Timestamp.from(Instant.now()));
            ps.setString(4, mapToJson(haStats.getMetadata()));
            ps.setString(5, mapToJson(haStats.getSettings()));
            ps.setString(6, mapToJson(haStats.getExt()));

            ps.executeUpdate();
        }
    }

    // ── Startup / Shutdown ──────────────────────────────────────────────

    @Override
    public void logStartupSummary() {
        int pendingMEs = countRows("SELECT COUNT(*) AS cnt FROM " + MATCHING_EVENT + " WHERE ha_uuid = ?");
        int pendingActions = countRows("SELECT COUNT(*) AS cnt FROM " + ACTION_INFO + " WHERE ha_uuid = ?");
        int sessionCount = countRows("SELECT COUNT(DISTINCT rule_set_name) AS cnt FROM " + SESSION_STATE + " WHERE ha_uuid = ?");
        int partialEvents = countPartialEventsInMemory();
        String leader = haStats != null ? haStats.getCurrentLeader() : "unknown";
        int switches = haStats != null ? haStats.getLeaderSwitches() : 0;

        logger.info("HA startup summary [ha_uuid={}, leader={}]: {} session(s), {} partial event(s), {} pending matching event(s), {} pending action(s), leader switches: {}",
                     haUuid, leader, sessionCount, partialEvents, pendingMEs, pendingActions, switches);
    }

    @Override
    public void shutdown() {
        deregisterShutdownHook();
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
            logger.info("Shutting down H2StateManager");
        }
    }

    /**
     * Force H2 to fully close the database and release all JVM-level caches.
     * This executes the H2 SHUTDOWN command before closing the connection pool.
     * Useful in tests to ensure file-backed databases are completely released
     * between test runs, preventing stale data from H2's internal database cache.
     */
    public void shutdownCompletely() {
        if (dataSource != null && !dataSource.isClosed()) {
            try (Connection conn = dataSource.getConnection();
                 var stmt = conn.createStatement()) {
                stmt.execute("SHUTDOWN");
            } catch (SQLException e) {
                logger.debug("H2 SHUTDOWN command failed (may already be closed): {}", e.getMessage());
            }
            dataSource.close();
            logger.info("Shutting down H2StateManager completely");
        }
    }

    // ── Counting / query helpers ────────────────────────────────────────

    private int countRows(String sql) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, haUuid);
            ResultSet rs = ps.executeQuery();

            if (rs.next()) {
                return rs.getInt(1);
            }
        } catch (SQLException e) {
            logger.warn("Failed to execute count query: {}", e.getMessage());
        }
        return 0;
    }

    private Integer fetchActionStatusFromDatabase(String matchingUuid, int index) {
        String sql = "SELECT action_data FROM " + ACTION_INFO + " WHERE me_uuid = ? AND index = ?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, matchingUuid);
            ps.setInt(2, index);

            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return extractStatus(decryptIfEnabled(rs.getString("action_data")));
            }
        } catch (SQLException e) {
            logger.error("Failed to fetch action status", e);
        }
        return null;
    }

    // ── JSON serialization helpers ──────────────────────────────────────

    private String haStatsToJson(HAStats stats) {
        Map<String, Object> props = new LinkedHashMap<>();
        props.put("current_leader", stats.getCurrentLeader());
        props.put("leader_switches", stats.getLeaderSwitches());
        props.put("current_term_started_at", stats.getCurrentTermStartedAt());
        props.put("events_processed_in_term", stats.getEventsProcessedInTerm());
        props.put("actions_processed_in_term", stats.getActionsProcessedInTerm());
        props.put("incomplete_matching_events", stats.getIncompleteMatchingEvents());
        props.put("partial_events_in_memory", stats.getPartialEventsInMemory());
        props.put("global_session_stats", stats.getGlobalSessionStats());
        props.put("partial_fulfilled_rules", stats.getPartialFulfilledRules());
        props.put("session_state_size", stats.getSessionStateSize());
        return toJson(props);
    }

    private void populateHAStatsFromJson(String json, HAStats stats) {
        if (json == null || json.isBlank() || "{}".equals(json)) {
            return;
        }
        Map<String, Object> props = jsonToMap(json);
        stats.setCurrentLeader((String) props.get("current_leader"));
        stats.setLeaderSwitches(getIntFromMap(props, "leader_switches"));
        stats.setCurrentTermStartedAt((String) props.get("current_term_started_at"));
        stats.setEventsProcessedInTerm(getIntFromMap(props, "events_processed_in_term"));
        stats.setActionsProcessedInTerm(getIntFromMap(props, "actions_processed_in_term"));
        stats.setIncompleteMatchingEvents(getIntFromMap(props, "incomplete_matching_events"));
        stats.setPartialEventsInMemory(getIntFromMap(props, "partial_events_in_memory"));
        Object gss = props.get("global_session_stats");
        if (gss != null) {
            stats.setGlobalSessionStats(readValue(toJson(gss),
                    org.drools.ansible.rulebook.integration.api.rulesengine.SessionStats.class));
        }
        stats.setPartialFulfilledRules(getIntFromMap(props, "partial_fulfilled_rules"));
        Object ssSize = props.get("session_state_size");
        stats.setSessionStateSize(ssSize instanceof Number ? ((Number) ssSize).longValue() : 0L);
    }

    private int getIntFromMap(Map<String, Object> map, String key) {
        Object value = map.get(key);
        return value instanceof Number ? ((Number) value).intValue() : 0;
    }

    private String mapToJson(Map<String, Object> map) {
        if (map == null || map.isEmpty()) return "{}";
        return toJson(map);
    }

    private Map<String, Object> jsonToMap(String json) {
        if (json == null || json.isBlank() || "{}".equals(json)) return new HashMap<>();
        try {
            return readValueAsMapOfStringAndObject(json);
        } catch (Exception e) {
            logger.warn("Failed to parse JSON map, returning empty: {}", e.getMessage());
            return new HashMap<>();
        }
    }

    private Integer extractStatus(String actionJson) {
        if (actionJson == null) {
            return null;
        }
        try {
            Object statusValue = readValueAsMapOfStringAndObject(actionJson).get("status");
            if (statusValue instanceof Number number) {
                return number.intValue();
            }
            if (statusValue instanceof String str && !str.isBlank()) {
                try {
                    return Integer.parseInt(str.trim());
                } catch (NumberFormatException nfe) {
                    logger.debug("Ignoring non-numeric status value: {}", str);
                }
            }
        } catch (Exception e) {
            logger.warn("Unable to parse action status from JSON", e);
        }
        return null;
    }

}
