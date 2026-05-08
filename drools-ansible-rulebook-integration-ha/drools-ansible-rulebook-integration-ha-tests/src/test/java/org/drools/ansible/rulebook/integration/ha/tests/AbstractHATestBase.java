package org.drools.ansible.rulebook.integration.ha.tests;

import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.PostgreSQLContainer;

import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.toJson;

/**
 * Abstract base class for all HA tests.
 * Provides common database setup logic for both H2 and PostgreSQL.
 * Supports switching databases via system property 'test.db.type'.
 *
 * Subclasses should call initializePostgres() or initializeH2() in their static block
 * with appropriate database name.
 *
 * As long as HA tests involve HAStateManager or AstRulesEngine, they should extend this class.
 *
 * Usage:
 * - Default (H2): mvn test
 * - PostgreSQL: mvn test -Dtest.db.type=postgres
 */
public abstract class AbstractHATestBase {

    // Determine database type from system property
    protected static final String TEST_DB_TYPE = System.getProperty("test.db.type", "h2");
    protected static final boolean USE_POSTGRES = "postgres".equalsIgnoreCase(TEST_DB_TYPE) ||
                                                "postgresql".equalsIgnoreCase(TEST_DB_TYPE);

    // PostgreSQL container (only initialized if USE_POSTGRES is true)
    protected static PostgreSQLContainer<?> postgres;

    // Database configuration (populated based on TEST_DB_TYPE)
    protected static Map<String, Object> dbParams;
    protected static Map<String, Object> dbHAConfig;

    // JSON strings for AstRulesEngine API (converted from Maps)
    protected static String dbParamsJson;
    protected static String dbHAConfigJson;

    // Per-test H2 file path (unique per test method to avoid file lock contention)
    protected String currentH2FilePath;

    /**
     * Clean up database before each test to ensure a fresh state.
     * For H2: generates a unique file path per test to avoid file lock contention in CI.
     * For PostgreSQL: drops all tables.
     */
    @BeforeEach
    void cleanupDatabaseBeforeTest() {
        if (USE_POSTGRES) {
            cleanupDatabase();
        } else {
            initializeH2ForTest();
        }
    }

    /**
     * Initialize PostgreSQL Testcontainer with specified database name.
     * This allows different test suites to use different databases for isolation.
     *
     * @param databaseName The name for the PostgreSQL database (e.g., "eda_ha_test", "eda_ha_unit_test")
     * @param testType Description for logging (e.g., "HA integration tests", "HAStateManager tests")
     */
    protected static void initializePostgres(String databaseName, String testType) {
        System.out.println("Initializing PostgreSQL Testcontainer for " + testType + "...");

        // Start Testcontainer
        postgres = new PostgreSQLContainer<>("postgres:15-alpine")
            .withDatabaseName(databaseName)
            .withUsername("test")
            .withPassword("test")
            .withReuse(true); // Reuse across test classes for performance

        postgres.start();

        // Configure parameters
        dbParams = Map.of(
            "db_type", "postgres",
            "host", postgres.getHost(),
            "port", postgres.getMappedPort(5432),
            "database", postgres.getDatabaseName(),
            "user", postgres.getUsername(),
            "password", postgres.getPassword(),
            "sslmode", "disable"
        );

        dbHAConfig = Map.of("write_after", 1);

        // Convert to JSON for AstRulesEngine API
        dbParamsJson = toJson(dbParams);
        dbHAConfigJson = toJson(dbHAConfig);

        // Configure TestUtils with PostgreSQL params
        TestUtils.setDbTestConfig(dbParams, dbHAConfig);

        // Register shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (postgres != null && postgres.isRunning()) {
                System.out.println("Stopping PostgreSQL Testcontainer...");
                postgres.stop();
            }
        }));

        System.out.println("PostgreSQL Testcontainer started at " +
            postgres.getHost() + ":" + postgres.getMappedPort(5432));
    }

    /**
     * Initialize H2 HA configuration (static part only).
     * The actual H2 file path is generated per test method in initializeH2ForTest().
     */
    protected static void initializeH2() {
        dbHAConfig = Map.of("write_after", 1);
        dbHAConfigJson = toJson(dbHAConfig);
    }

    /**
     * Initialize a unique H2 file path for the current test method.
     * Each test gets its own database file to avoid file lock contention in CI,
     * where quick test restarts can leave stale H2 file locks.
     * Both HA nodes within a single test share the same file (required for failover).
     */
    protected void initializeH2ForTest() {
        currentH2FilePath = TestUtils.generateUniqueH2FilePath();
        System.out.println("Using H2 file-backed database: " + currentH2FilePath);

        dbParams = Map.of(
            "db_type", "h2",
            "db_file_path", currentH2FilePath
        );
        dbParamsJson = toJson(dbParams);
    }

    /**
     * Clean up database based on database type.
     * For H2: deletes the database files.
     * For PostgreSQL: drops all tables.
     */
    protected void cleanupDatabase() {
        if (USE_POSTGRES) {
            TestUtils.dropPostgresTables(dbParams);
        } else if (currentH2FilePath != null) {
            TestUtils.shutdownH2Database(currentH2FilePath);
            TestUtils.deleteH2Files(currentH2FilePath);
        }
    }
}
