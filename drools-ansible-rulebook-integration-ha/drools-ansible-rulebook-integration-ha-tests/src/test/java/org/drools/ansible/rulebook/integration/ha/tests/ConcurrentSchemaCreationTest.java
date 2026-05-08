package org.drools.ansible.rulebook.integration.ha.tests;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.drools.ansible.rulebook.integration.ha.postgres.PostgreSQLSchema;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

/**
 * Tests concurrent and sequential calls to PostgreSQLSchema.createSchema() and migrateSchema().
 * <p>
 * Uses PostgreSQL via Testcontainers to verify real concurrent DDL behavior:
 * <ul>
 *   <li>Concurrent schema creation: both threads succeed (PostgreSQL handles IF NOT EXISTS correctly)</li>
 *   <li>Sequential (idempotent) schema creation: second call succeeds and produces SQLWarning</li>
 *   <li>Concurrent create+migrate: both threads succeed</li>
 * </ul>
 */
@EnabledIfSystemProperty(named = "test.db.type", matches = "postgres(ql)?")
class ConcurrentSchemaCreationTest {

    private static final Logger logger = LoggerFactory.getLogger(ConcurrentSchemaCreationTest.class);

    private static PostgreSQLContainer<?> postgres;
    private static HikariDataSource dataSource;

    @BeforeAll
    static void startPostgres() {
        postgres = new PostgreSQLContainer<>("postgres:15-alpine")
                .withDatabaseName("concurrent_schema_test")
                .withUsername("test")
                .withPassword("test");
        postgres.start();

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(postgres.getJdbcUrl());
        config.setUsername(postgres.getUsername());
        config.setPassword(postgres.getPassword());
        config.setMaximumPoolSize(10);
        config.setDriverClassName("org.postgresql.Driver");

        dataSource = new HikariDataSource(config);

        logger.info("PostgreSQL Testcontainer started at {}:{}", postgres.getHost(), postgres.getMappedPort(5432));
    }

    @AfterAll
    static void stopPostgres() {
        if (dataSource != null) {
            dataSource.close();
        }
        if (postgres != null && postgres.isRunning()) {
            postgres.stop();
        }
    }

    @AfterEach
    void dropSchema() throws Exception {
        PostgreSQLSchema.dropSchema(dataSource);
    }

    @Test
    void testConcurrentCreateSchema_bothSucceed() throws Exception {
        CyclicBarrier barrier = new CyclicBarrier(2);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);

        List<Future<Void>> futures = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            futures.add(executor.submit(() -> {
                barrier.await();
                PostgreSQLSchema.createSchema(dataSource);
                successCount.incrementAndGet();
                return null;
            }));
        }

        for (Future<Void> future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                failureCount.incrementAndGet();
                logger.warn("Concurrent createSchema thread failed: {}", e.getCause().getMessage());
            }
        }
        executor.shutdown();

        logger.info("Concurrent createSchema result: {} succeeded, {} failed", successCount.get(), failureCount.get());
        assertThat(successCount.get()).as("Both threads should succeed with PostgreSQL").isEqualTo(2);

        assertSchemaTablesExist();
    }

    @Test
    void testCreateSchema_sequentialSecondCallIsIdempotent() throws Exception {
        // First call creates the schema
        PostgreSQLSchema.createSchema(dataSource);

        // Second sequential call should succeed (IF NOT EXISTS idempotency)
        assertThatNoException().isThrownBy(() -> PostgreSQLSchema.createSchema(dataSource));

        // Verify schema is still correct after double creation
        assertSchemaTablesExist();
    }

    @Test
    void testConcurrentCreateAndMigrateSchema_bothSucceed() throws Exception {
        CyclicBarrier barrier = new CyclicBarrier(2);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);

        List<Future<Void>> futures = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            futures.add(executor.submit(() -> {
                barrier.await();
                PostgreSQLSchema.createSchema(dataSource);
                PostgreSQLSchema.migrateSchema(dataSource);
                successCount.incrementAndGet();
                return null;
            }));
        }

        for (Future<Void> future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                failureCount.incrementAndGet();
                logger.warn("Concurrent createSchema+migrateSchema thread failed: {}", e.getCause().getMessage());
            }
        }
        executor.shutdown();

        logger.info("Concurrent createSchema+migrateSchema result: {} succeeded, {} failed", successCount.get(), failureCount.get());
        assertThat(successCount.get()).as("Both threads should succeed with PostgreSQL").isEqualTo(2);

        assertSchemaTablesExist();
    }

    /**
     * Verifies all expected HA tables exist in the database.
     */
    private void assertSchemaTablesExist() throws Exception {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            for (String table : new String[]{
                    "drools_ansible_session_state",
                    "drools_ansible_matching_event",
                    "drools_ansible_action_info",
                    "drools_ansible_ha_stats"}) {
                ResultSet rs = stmt.executeQuery(
                        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '" + table + "'");
                rs.next();
                assertThat(rs.getInt(1)).as("Table %s should exist", table).isEqualTo(1);
            }
        }
    }
}
