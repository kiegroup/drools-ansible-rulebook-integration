package org.drools.ansible.rulebook.integration.ha.tests.db.postgres;

import org.drools.ansible.rulebook.integration.ha.tests.support.TestUtils;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import org.drools.ansible.rulebook.integration.ha.api.HAStateManager;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManagerFactory;
import org.drools.ansible.rulebook.integration.ha.api.HAUtils;
import org.drools.ansible.rulebook.integration.ha.model.MatchingEvent;
import org.drools.ansible.rulebook.integration.ha.model.SessionState;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.drools.ansible.rulebook.integration.ha.tests.support.TestUtils.createMatchingEvent;

/**
 * Integration test for PostgreSQL multi-host connection support.
 * Verifies that the PostgreSQLStateManager correctly builds multi-host JDBC URLs
 * and that pgjdbc can connect through them.
 *
 * Always runs against PostgreSQL (not H2) — skips if Docker is not available.
 */
class HAMultiHostPostgresTest {

    private static final String HA_UUID = "multi-host-test-1";
    private static final String WORKER_NAME = "multi-host-leader";

    private static PostgreSQLContainer<?> postgres;
    private static String pgHost;
    private static int pgPort;

    private HAStateManager stateManager;

    @BeforeAll
    static void startPostgres() {
        postgres = new PostgreSQLContainer<>("postgres:15-alpine")
                .withDatabaseName("eda_ha_multihost_test")
                .withUsername("test")
                .withPassword("test");
        postgres.start();

        pgHost = postgres.getHost();
        pgPort = postgres.getMappedPort(5432);
    }

    @BeforeEach
    void cleanTables() {
        Map<String, Object> cleanupParams = Map.of(
                "host", pgHost,
                "port", pgPort,
                "database", postgres.getDatabaseName(),
                "user", postgres.getUsername(),
                "password", postgres.getPassword()
        );
        try {
            TestUtils.dropPostgresTables(cleanupParams);
        } catch (Exception ignored) {
            // Tables may not exist yet on first run
        }
    }

    @AfterEach
    void tearDown() {
        if (stateManager != null) {
            stateManager.shutdown();
            stateManager = null;
        }
    }

    // ── Multi-host with single port ─────────────────────────────────────

    @Test
    void testMultiHostSinglePort_connectsAndPersists() {
        // Pass the same host twice (simulating two-node cluster where first is reachable)
        // pgjdbc will connect to the first host successfully
        String multiHost = pgHost + "," + pgHost;
        String port = String.valueOf(pgPort);

        Map<String, Object> params = buildDbParams(multiHost, port, null);
        initAndVerifyRoundTrip(params);
    }

    // ── Multi-host with per-host ports ──────────────────────────────────

    @Test
    void testMultiHostMultiPort_connectsAndPersists() {
        // Both hosts point to the same container, each with its own port entry
        String multiHost = pgHost + "," + pgHost;
        String multiPort = pgPort + "," + pgPort;

        Map<String, Object> params = buildDbParams(multiHost, multiPort, null);
        initAndVerifyRoundTrip(params);
    }

    // ── Multi-host with target_session_attrs ────────────────────────────

    @Test
    void testMultiHost_targetSessionAttrsAny() {
        String multiHost = pgHost + "," + pgHost;

        Map<String, Object> params = buildDbParams(multiHost, String.valueOf(pgPort), "any");
        initAndVerifyRoundTrip(params);
    }

    @Test
    void testMultiHost_targetSessionAttrsReadWrite() {
        // "read-write" maps to targetServerType=primary
        // A standalone postgres accepts writes, so it qualifies as "primary"
        String multiHost = pgHost + "," + pgHost;

        Map<String, Object> params = buildDbParams(multiHost, String.valueOf(pgPort), "read-write");
        initAndVerifyRoundTrip(params);
    }

    // ── Failover: first host unreachable, second reachable ──────────────

    @Test
    void testMultiHost_failoverToSecondHost() throws IOException {
        // First host is an invalid port (unreachable), second host is the real container.
        // pgjdbc should fail on the first and connect to the second.
        //
        // Use a fresh ephemeral port each time to avoid pgjdbc's GlobalHostStatusTracker
        // cache (hostRecheckSeconds=10s). Bind-then-close guarantees the port is allocated
        // but nothing is listening on it.
        int deadPort;
        try (ServerSocket ss = new ServerSocket(0)) {
            deadPort = ss.getLocalPort();
        }

        String multiHost = pgHost + "," + pgHost;
        String multiPort = deadPort + "," + pgPort;

        // Capture pgjdbc JUL logs to verify failover behavior.
        // pgjdbc logs via java.util.logging under "org.postgresql" and child loggers
        // (e.g., "org.postgresql.core.v3"). We install our handler on the root "org.postgresql"
        // logger and ensure child messages propagate up to it.
        Logger pgLogger = Logger.getLogger("org.postgresql");
        Level originalLevel = pgLogger.getLevel();
        boolean originalUseParent = pgLogger.getUseParentHandlers();
        pgLogger.setLevel(Level.ALL);
        pgLogger.setUseParentHandlers(false); // prevent duplicate output to console
        List<String> logMessages = Collections.synchronizedList(new ArrayList<>());
        Handler captureHandler = new Handler() {
            @Override public void publish(LogRecord record) {
                logMessages.add(record.getMessage());
            }
            @Override public void flush() {}
            @Override public void close() {}
        };
        captureHandler.setLevel(Level.ALL);
        pgLogger.addHandler(captureHandler);

        try {
            Map<String, Object> params = buildDbParams(multiHost, multiPort, null);
            initAndVerifyRoundTrip(params);

            // Verify pgjdbc tried two hosts: logged "Trying to establish" twice
            List<String> logSnapshot;
            synchronized (logMessages) {
                logSnapshot = new ArrayList<>(logMessages);
            }

            long tryCount = logSnapshot.stream()
                    .filter(msg -> msg.contains("Trying to establish"))
                    .count();
            assertThat(tryCount).as("pgjdbc should attempt connection to both hosts")
                    .isGreaterThanOrEqualTo(2);

            // Verify pgjdbc logged a ConnectException for the unreachable first host
            assertThat(logSnapshot.stream().anyMatch(msg -> msg.contains("ConnectException")))
                    .as("pgjdbc should log ConnectException for the unreachable host")
                    .isTrue();
        } finally {
            pgLogger.removeHandler(captureHandler);
            pgLogger.setLevel(originalLevel);
            pgLogger.setUseParentHandlers(originalUseParent);
        }
    }

    // ── Single-host backward compatibility ──────────────────────────────

    @Test
    void testSingleHost_backwardCompatible() {
        // Verify that single-host with port-as-string still works
        Map<String, Object> params = buildDbParams(pgHost, String.valueOf(pgPort), null);
        initAndVerifyRoundTrip(params);
    }

    @Test
    void testSingleHost_portAsInteger_backwardCompatible() {
        // Verify that port passed as Integer (the old format) still works
        Map<String, Object> params = new HashMap<>();
        params.put("db_type", "postgres");
        params.put("host", pgHost);
        params.put("port", pgPort); // Integer
        params.put("database", postgres.getDatabaseName());
        params.put("user", postgres.getUsername());
        params.put("password", postgres.getPassword());
        params.put("sslmode", "disable");
        initAndVerifyRoundTrip(params);
    }

    // ── Helpers ─────────────────────────────────────────────────────────

    private Map<String, Object> buildDbParams(String host, String port, String targetSessionAttrs) {
        Map<String, Object> params = new HashMap<>();
        params.put("db_type", "postgres");
        params.put("host", host);
        params.put("port", port);
        params.put("database", postgres.getDatabaseName());
        params.put("user", postgres.getUsername());
        params.put("password", postgres.getPassword());
        params.put("sslmode", "disable");
        if (targetSessionAttrs != null) {
            params.put("target_session_attrs", targetSessionAttrs);
        }
        return params;
    }

    /**
     * Initialize a state manager with the given params, perform a full round-trip
     * (persist session state, add matching event, read back), then verify correctness.
     */
    private void initAndVerifyRoundTrip(Map<String, Object> params) {
        Map<String, Object> haConfig = Map.of("write_after", 1);

        stateManager = HAStateManagerFactory.create("postgres");
        stateManager.initializeHA(HA_UUID, WORKER_NAME, params, haConfig);
        stateManager.enableLeader();

        // Persist a session state
        SessionState ss = new SessionState();
        ss.setHaUuid(HA_UUID);
        ss.setRuleSetName("multihost-ruleset");
        ss.setRulebookHash("abc123");
        ss.setLeaderId(WORKER_NAME);
        long now = System.currentTimeMillis();
        ss.setCreatedTime(now);
        ss.setPersistedTime(now);
        ss.setCurrentStateSHA(HAUtils.calculateStateSHA(ss));
        stateManager.persistSessionState(ss);

        // Read it back
        SessionState loaded = stateManager.getPersistedSessionState("multihost-ruleset");
        assertThat(loaded).isNotNull();
        assertThat(loaded.getRulebookHash()).isEqualTo("abc123");
        assertThat(loaded.getLeaderId()).isEqualTo(WORKER_NAME);

        // Add a matching event
        MatchingEvent me = createMatchingEvent(HA_UUID, "multihost-ruleset", "rule1",
                Map.of("sensor", "temp", "value", 42));
        String meUuid = stateManager.addMatchingEvent(me);
        assertThat(meUuid).isNotNull();

        // Read pending events
        List<MatchingEvent> pending = stateManager.getPendingMatchingEvents();
        assertThat(pending).hasSize(1);
        assertThat(pending.get(0).getRuleName()).isEqualTo("rule1");
    }
}
