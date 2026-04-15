package org.drools.ansible.rulebook.integration.ha.tests.db.postgres;

import org.drools.ansible.rulebook.integration.ha.tests.ssl.SSLTestCertificateGenerator;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import org.drools.ansible.rulebook.integration.ha.api.HAStateManager;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManagerFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration test for an SSL-only PostgreSQL server (no non-SSL fallback for user 'test').
 * <p>
 * The pg_hba.conf has no {@code host all test ...} rule, so {@code sslmode=disable} must fail.
 * This verifies that the server rejects non-SSL connections when no fallback is configured.
 * <p>
 * Run with: {@code mvn test -pl drools-ansible-rulebook-integration-ha/drools-ansible-rulebook-integration-ha-tests
 * -Dtest=HAPostgresSSLOnlyTest -Dtest.db.type=postgres}
 */
@EnabledIfSystemProperty(named = "test.db.type", matches = "postgres(ql)?")
class HAPostgresSSLOnlyTest {

    private static final String WORKER_NAME = "ssl-only-worker-1";

    private static PostgreSQLContainer<?> sslOnlyPostgres;
    private static SSLTestCertificateGenerator.CertBundle bundle;
    private static Path tempDir;

    @BeforeAll
    static void setUp() throws Exception {
        tempDir = Files.createTempDirectory("ha-ssl-only-test-");
        bundle = SSLTestCertificateGenerator.generate(tempDir.resolve("certs"));

        sslOnlyPostgres = createSSLOnlyPostgresContainer(bundle);
        sslOnlyPostgres.start();
    }

    @AfterAll
    static void tearDown() {
        if (sslOnlyPostgres != null && sslOnlyPostgres.isRunning()) {
            sslOnlyPostgres.stop();
        }
    }

    @Test
    void testSslOnlyServerRejectsDisabledSsl() throws Exception {
        String haUuid = "ssl-test-ssl-only-disable";
        Map<String, Object> dbParams = new HashMap<>();
        dbParams.put("db_type", "postgres");
        dbParams.put("host", sslOnlyPostgres.getHost());
        dbParams.put("port", sslOnlyPostgres.getMappedPort(5432));
        dbParams.put("database", "eda_ha_ssl_test");
        dbParams.put("user", "test");
        dbParams.put("password", "test");
        dbParams.put("sslmode", "disable");

        HAStateManager stateManager = HAStateManagerFactory.create("postgres");
        assertThatThrownBy(() ->
                stateManager.initializeHA(haUuid, WORKER_NAME, dbParams, Map.of("write_after", 1)))
                .isInstanceOf(RuntimeException.class);
    }

    /**
     * Creates a PostgreSQL container configured for SSL-only connections for user 'test'.
     * No non-SSL fallback rules for 'test' in pg_hba.conf.
     * The 'postgres' superuser retains non-SSL access for Testcontainers healthcheck.
     */
    private static PostgreSQLContainer<?> createSSLOnlyPostgresContainer(
            SSLTestCertificateGenerator.CertBundle certs) throws Exception {
        byte[] caCertBytes = Files.readAllBytes(certs.caCert());
        byte[] serverKeyBytes = Files.readAllBytes(certs.serverKey());
        byte[] serverCertBytes = Files.readAllBytes(certs.serverCert());

        String initScript = """
                #!/bin/bash
                set -e

                # Create test user and grant access
                psql -v ON_ERROR_STOP=1 --username postgres --dbname eda_ha_ssl_test <<'EOSQL'
                CREATE USER test WITH PASSWORD 'test';
                GRANT ALL PRIVILEGES ON DATABASE eda_ha_ssl_test TO test;
                GRANT ALL ON SCHEMA public TO test;
                EOSQL

                # Copy certs to PGDATA
                cp /tmp/ssl/server.key "$PGDATA/server.key"
                cp /tmp/ssl/server.crt "$PGDATA/server.crt"
                cp /tmp/ssl/ca.crt "$PGDATA/ca.crt"

                chmod 600 "$PGDATA/server.key"
                chown postgres:postgres "$PGDATA/server.key" "$PGDATA/server.crt" "$PGDATA/ca.crt"

                # Enable SSL in postgresql.conf
                echo "ssl = on" >> "$PGDATA/postgresql.conf"
                echo "ssl_cert_file = 'server.crt'" >> "$PGDATA/postgresql.conf"
                echo "ssl_key_file = 'server.key'" >> "$PGDATA/postgresql.conf"
                echo "ssl_ca_file = 'ca.crt'" >> "$PGDATA/postgresql.conf"

                # pg_hba.conf: SSL-only for 'test', non-SSL for 'postgres' (healthcheck)
                cat > "$PGDATA/pg_hba.conf" << 'PGEOF'
                local   all   all                       trust
                hostssl all   test      0.0.0.0/0        cert
                hostssl all   test      ::/0             cert
                host    all   postgres  0.0.0.0/0        scram-sha-256
                host    all   postgres  ::/0             scram-sha-256
                PGEOF

                pg_ctl reload -D "$PGDATA"
                """;

        return new PostgreSQLContainer<>(DockerImageName.parse("postgres:15-alpine"))
                .withDatabaseName("eda_ha_ssl_test")
                .withUsername("postgres")  // healthcheck uses superuser (has non-SSL rule)
                .withPassword("postgres")
                .withCopyToContainer(Transferable.of(serverKeyBytes), "/tmp/ssl/server.key")
                .withCopyToContainer(Transferable.of(serverCertBytes), "/tmp/ssl/server.crt")
                .withCopyToContainer(Transferable.of(caCertBytes), "/tmp/ssl/ca.crt")
                .withCopyToContainer(Transferable.of(initScript), "/docker-entrypoint-initdb.d/00-ssl-setup.sh");
    }
}
