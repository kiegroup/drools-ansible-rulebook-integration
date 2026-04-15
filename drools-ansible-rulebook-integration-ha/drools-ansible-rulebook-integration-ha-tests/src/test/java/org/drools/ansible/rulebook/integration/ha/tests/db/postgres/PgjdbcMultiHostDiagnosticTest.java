package org.drools.ansible.rulebook.integration.ha.tests.db.postgres;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Diagnostic test to observe pgjdbc multi-host failover behavior at the JDBC driver level.
 */
class PgjdbcMultiHostDiagnosticTest {

    private static PostgreSQLContainer<?> postgres;

    @BeforeAll
    static void start() {
        postgres = new PostgreSQLContainer<>("postgres:15-alpine")
                .withDatabaseName("diag_test")
                .withUsername("test")
                .withPassword("test");
        postgres.start();
    }

    @AfterAll
    static void stop() {
        if (postgres != null) {
            postgres.stop();
        }
    }

    @Test
    void observeFailoverWithDriverLog() throws Exception {
        String host = postgres.getHost();
        int goodPort = postgres.getMappedPort(5432);

        // Capture pgjdbc JUL logs
        Logger pgLogger = Logger.getLogger("org.postgresql");
        pgLogger.setLevel(Level.ALL);
        List<String> logMessages = new ArrayList<>();
        Handler captureHandler = new Handler() {
            @Override public void publish(LogRecord record) {
                logMessages.add(record.getLevel() + ": " + record.getMessage());
            }
            @Override public void flush() {}
            @Override public void close() {}
        };
        captureHandler.setLevel(Level.ALL);
        pgLogger.addHandler(captureHandler);

        try {
            // Multi-host URL: first host is unreachable (port 59999), second is the real container
            String url = String.format(
                    "jdbc:postgresql://%s:59999,%s:%d/diag_test?targetServerType=any&connectTimeout=5",
                    host, host, goodPort);

            System.out.println("=== JDBC URL: " + url);
            System.out.println("=== Attempting multi-host connection...");

            long startTime = System.currentTimeMillis();
            try (Connection conn = DriverManager.getConnection(url, "test", "test")) {
                long elapsed = System.currentTimeMillis() - startTime;
                System.out.println("=== Connected in " + elapsed + " ms");

                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery("SELECT inet_server_port()")) {
                    rs.next();
                    System.out.println("=== Connected to server port: " + rs.getInt(1) + " (container internal port)");
                }

                assertThat(conn.isValid(5)).isTrue();
            }

            // Print captured pgjdbc logs
            System.out.println("=== pgjdbc log messages (" + logMessages.size() + " entries):");
            for (String msg : logMessages) {
                System.out.println("  pgjdbc> " + msg);
            }
        } finally {
            pgLogger.removeHandler(captureHandler);
        }
    }

    @Test
    void singleHostReference() throws Exception {
        String host = postgres.getHost();
        int goodPort = postgres.getMappedPort(5432);

        String url = String.format(
                "jdbc:postgresql://%s:%d/diag_test?targetServerType=any",
                host, goodPort);

        System.out.println("=== Single-host JDBC URL: " + url);

        long startTime = System.currentTimeMillis();
        try (Connection conn = DriverManager.getConnection(url, "test", "test")) {
            long elapsed = System.currentTimeMillis() - startTime;
            System.out.println("=== Single-host connected in " + elapsed + " ms");

            assertThat(conn.isValid(5)).isTrue();
        }
    }
}
