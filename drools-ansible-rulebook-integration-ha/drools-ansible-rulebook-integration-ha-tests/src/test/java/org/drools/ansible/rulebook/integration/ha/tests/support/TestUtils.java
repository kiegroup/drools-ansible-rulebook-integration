package org.drools.ansible.rulebook.integration.ha.tests.support;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.drools.ansible.rulebook.integration.api.io.JsonMapper;
import org.drools.ansible.rulebook.integration.ha.postgres.PostgreSQLSchema;
import org.drools.ansible.rulebook.integration.ha.model.MatchingEvent;

import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.toJson;

public class TestUtils {

    // H2 file-backed database path for tests (shared between nodes via same file)
    public static final String TEST_H2_FILE_PATH = "./target/h2-test/eda_ha";

    // PostgreSQL Configuration (for Testcontainers)
    // These will be populated by PostgreSQLTestBase when container starts
    private static volatile Map<String, Object> dbParams = null;
    private static volatile Map<String, Object> dbHAConfig = null;

    public static void setDbTestConfig(Map<String, Object> params, Map<String, Object> config) {
        dbParams = params;
        dbHAConfig = config;
    }

    public static Map<String, Object> getDbParams() {
        return dbParams != null ? dbParams : new HashMap<>();
    }

    public static Map<String, Object> getDbHAConfig() {
        return dbHAConfig != null ? dbHAConfig : Map.of("write_after", 1);
    }

    private TestUtils() {
    }

    public static MatchingEvent createMatchingEvent(String haUuid, String rulesetName,
                                              String ruleName, Map<String, Object> matchingFacts) {
        MatchingEvent me = new MatchingEvent();
        me.setHaUuid(haUuid);
        me.setRuleSetName(rulesetName);
        me.setRuleName(ruleName);

        // Serialize matching facts to JSON
        String eventDataJson = toJson(matchingFacts);
        me.setEventData(eventDataJson);
        return me;
    }

    /**
     * Generate a unique H2 file path for test isolation.
     * Each test method gets its own database file, avoiding file lock contention in CI.
     */
    public static String generateUniqueH2FilePath() {
        return "./target/h2-test/eda_ha_" + UUID.randomUUID().toString().substring(0, 8);
    }

    /**
     * Force H2 to fully close the database at the default test file path.
     */
    public static void shutdownH2Database() {
        shutdownH2Database(TEST_H2_FILE_PATH);
    }

    /**
     * Force H2 to fully close the database and remove it from the JVM-level cache.
     * H2 maintains an internal static map of open databases. Without an explicit SHUTDOWN,
     * reopening the same file path may return cached (stale) data even after file deletion.
     * Uses IFEXISTS=TRUE to avoid creating a new database if none exists.
     */
    public static void shutdownH2Database(String h2FilePath) {
        String jdbcUrl = "jdbc:h2:file:" + h2FilePath + ";MODE=PostgreSQL;IFEXISTS=TRUE";
        try (var conn = DriverManager.getConnection(jdbcUrl, "sa", "");
             var stmt = conn.createStatement()) {
            stmt.execute("SHUTDOWN");
        } catch (Exception e) {
            // Database might already be closed or file doesn't exist - that's OK
        }
    }

    /**
     * Delete H2 database files for the default test file path.
     */
    public static void deleteH2Files() {
        deleteH2Files(TEST_H2_FILE_PATH);
    }

    /**
     * Delete H2 database files for the given file path.
     * H2 creates .mv.db and optionally .trace.db files.
     */
    public static void deleteH2Files(String h2FilePath) {
        Path dir = Path.of(h2FilePath).getParent();
        if (dir == null || !Files.exists(dir)) {
            return;
        }
        String baseName = Path.of(h2FilePath).getFileName().toString();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, baseName + ".*")) {
            for (Path file : stream) {
                Files.deleteIfExists(file);
            }
        } catch (IOException e) {
            System.err.println("Failed to delete H2 files: " + e.getMessage());
        }
    }

    public static void dropPostgresTables() {
        dropPostgresTables(dbParams);
    }

    public static void dropPostgresTables(Map<String, Object> params) {
        if (params == null || params.isEmpty()) {
            return; // No PostgreSQL configured
        }

        String host = (String) params.get("host");
        Integer port = (Integer) params.get("port");
        String database = (String) params.get("database");
        String username = (String) params.get("user");
        String password = (String) params.get("password");

        String jdbcUrl = String.format("jdbc:postgresql://%s:%d/%s", host, port, database);

        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(jdbcUrl);
        hikariConfig.setUsername(username);
        hikariConfig.setPassword(password);
        hikariConfig.setMaximumPoolSize(1);  // Only need 1 connection for dropping tables

        try (HikariDataSource dataSource = new HikariDataSource(hikariConfig)) {
            PostgreSQLSchema.dropSchema(dataSource);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to drop PostgreSQL tables", e);
        }
    }

    /**
     * Query a raw column value from the database via direct JDBC, bypassing the state manager's decryption.
     * Useful for verifying that data is actually encrypted at rest.
     *
     * @param params database connection parameters (must contain db_type, and for postgres: host, port, database, user, password)
     * @param sql SQL query with a single ? placeholder
     * @param paramValue value to bind to the ? placeholder
     * @return the first column value of the first row
     */
    public static String queryRawColumn(Map<String, Object> params, String sql, String paramValue) {
        String jdbcUrl;
        String user;
        String password;
        if ("postgres".equalsIgnoreCase((String) params.get("db_type"))) {
            String host = (String) params.get("host");
            Integer port = (Integer) params.get("port");
            String database = (String) params.get("database");
            jdbcUrl = String.format("jdbc:postgresql://%s:%d/%s", host, port, database);
            user = (String) params.get("user");
            password = (String) params.get("password");
        } else {
            String dbFilePath = (String) params.getOrDefault("db_file_path", TEST_H2_FILE_PATH);
            jdbcUrl = "jdbc:h2:file:" + dbFilePath + ";MODE=PostgreSQL";
            user = "sa";
            password = "";
        }
        boolean isPostgres = "postgres".equalsIgnoreCase((String) params.get("db_type"));
        try (Connection conn = DriverManager.getConnection(jdbcUrl, user, password);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            if (isPostgres) {
                try {
                    ps.setObject(1, UUID.fromString(paramValue));
                } catch (IllegalArgumentException e) {
                    ps.setString(1, paramValue);
                }
            } else {
                ps.setString(1, paramValue);
            }
            ResultSet rs = ps.executeQuery();
            if (!rs.next()) {
                throw new RuntimeException("No rows returned for query: " + sql + " with param: " + paramValue);
            }
            return rs.getString(1);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to read raw database", e);
        }
    }

    // Add meta/uuid to event body
    public static String createEvent(String eventBody) {
        String eventUuid = UUID.randomUUID().toString();
        return eventBody.replaceFirst("\\{", "{\"meta\": {\"uuid\": \"" + eventUuid + "\"}, ");
    }

    /**
     * Extract matching_uuid from HA mode response JSON.
     * HA response format: [{"name": "rule_name", "events": {...}, "matching_uuid": "uuid-here"}]
     *
     * @param response JSON response from assertEvent/assertFact in HA mode
     * @return matching_uuid string, or null if not found or response is empty
     */
    public static String extractMatchingUuidFromResponse(String response) {
        try {
            List<Map<String, Object>> matchList = JsonMapper.readValueAsListOfMapOfStringAndObject(response);
            if (matchList.isEmpty()) {
                return null;
            }
            Map<String, Object> firstMatch = matchList.get(0);
            return (String) firstMatch.get("matching_uuid");
        } catch (Exception e) {
            throw new RuntimeException("Failed to extract matching_uuid from response: " + response, e);
        }
    }

    /**
     * Extract matching_uuid from async recovery message JSON.
     * Async recovery message format:
     * {
     *   "session_id": 1,
     *   "result": {
     *     "matching_uuid": "uuid-here",
     *     "name": "rule_name",
     *     "type": "MATCHING_EVENT_RECOVERY",
     *     "ruleset_name": "ruleset_name",
     *     "events": {...}
     *   }
     * }
     *
     * @param asyncMessage JSON async recovery message
     * @return matching_uuid string, or null if not found
     */
    public static String extractMatchingUuidFromAsyncRecoveryMessage(String asyncMessage) {
        try {
            Map<String, Object> message = JsonMapper.readValueAsMapOfStringAndObject(asyncMessage);
            Map<String, Object> result = (Map<String, Object>) message.get("result");
            if (result == null) {
                return null;
            }
            return (String) result.get("matching_uuid");
        } catch (Exception e) {
            throw new RuntimeException("Failed to extract matching_uuid from async recovery message: " + asyncMessage, e);
        }
    }
}
