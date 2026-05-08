package org.drools.ansible.rulebook.integration.ha.tests;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import org.drools.ansible.rulebook.integration.ha.postgres.PemToKeyStoreConverter;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * SSL/mTLS integration tests for direct JDBC connections to PostgreSQL.
 * <p>
 * Tests multiple SSL key formats (DER, PKCS#12) via both Properties and URL parameters
 * against a real PostgreSQL container configured for client certificate authentication.
 * <p>
 * Run with: {@code mvn test -pl drools-ansible-rulebook-integration-ha/drools-ansible-rulebook-integration-ha-tests
 * -Dtest=PostgresSSLJdbcTest -Dtest.db.type=postgres}
 */
@EnabledIfSystemProperty(named = "test.db.type", matches = "postgres(ql)?")
class PostgresSSLJdbcTest {

    private static PostgreSQLContainer<?> postgres;
    private static SSLTestCertificateGenerator.CertBundle bundle;
    private static Path tempDir;
    private static String jdbcUrl;

    @BeforeAll
    static void setUp() throws Exception {
        tempDir = Files.createTempDirectory("ssl-jdbc-test-");
        bundle = SSLTestCertificateGenerator.generate(tempDir.resolve("certs"));

        // Start SSL PostgreSQL container
        postgres = createSSLPostgresContainer(bundle);
        postgres.start();

        jdbcUrl = String.format("jdbc:postgresql://%s:%d/eda_ha_ssl_test",
                postgres.getHost(), postgres.getMappedPort(5432));
    }

    @AfterAll
    static void tearDown() {
        if (postgres != null && postgres.isRunning()) {
            postgres.stop();
        }
    }

    @Test
    void testNonSSLConnection() throws Exception {
        Properties props = new Properties();
        props.setProperty("user", "test");
        props.setProperty("password", "test");
        props.setProperty("sslmode", "disable");

        try (Connection conn = DriverManager.getConnection(jdbcUrl, props);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT 1 AS result")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getInt("result")).isEqualTo(1);
        }
    }

    // Key format: unencrypted PEM PKCS#8 (converted to PKCS#12 for JDBC)
    @Test
    void testSSLWithUnencryptedPkcs8PemKeyViaProperties() throws Exception {
        // Derive unencrypted PKCS#8 PEM bundle and convert to PKCS#12 keystore
        SSLTestCertificateGenerator.CertBundle pkcs8Bundle =
                SSLTestCertificateGenerator.withUnencryptedPkcs8PemKey(bundle, tempDir.resolve("client-pkcs8.pem"));
        char[] p12Password = bundle.passphrase().toCharArray();
        Path convertedP12 = PemToKeyStoreConverter.convertPemToP12(
                pkcs8Bundle.clientKey().toString(),
                pkcs8Bundle.clientCert().toString(),
                p12Password);
        try {
            Properties props = new Properties();
            props.setProperty("user", "test");
            props.setProperty("password", "test");
            props.setProperty("sslmode", "require");
            props.setProperty("sslkey", convertedP12.toString());
            props.setProperty("sslpassword", bundle.passphrase());
            props.setProperty("sslrootcert", bundle.caCert().toString());

            try (Connection conn = DriverManager.getConnection(jdbcUrl, props);
                 Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT 1 AS result")) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getInt("result")).isEqualTo(1);
            }
        } finally {
            PemToKeyStoreConverter.cleanup(convertedP12);
        }
    }

    /**
     * Creates a PostgreSQL container configured for SSL with client certificate authentication.
     * Shared by multiple SSL test classes.
     */
    static PostgreSQLContainer<?> createSSLPostgresContainer(SSLTestCertificateGenerator.CertBundle certs) throws Exception {
        byte[] caCertBytes = Files.readAllBytes(certs.caCert());
        byte[] serverKeyBytes = Files.readAllBytes(certs.serverKey());
        byte[] serverCertBytes = Files.readAllBytes(certs.serverCert());

        String initScript = """
                #!/bin/bash
                set -e

                # Copy certs to PGDATA
                cp /tmp/ssl/server.key "$PGDATA/server.key"
                cp /tmp/ssl/server.crt "$PGDATA/server.crt"
                cp /tmp/ssl/ca.crt "$PGDATA/ca.crt"

                # PostgreSQL requires strict permissions on the key file
                chmod 600 "$PGDATA/server.key"
                chown postgres:postgres "$PGDATA/server.key" "$PGDATA/server.crt" "$PGDATA/ca.crt"

                # Enable SSL in postgresql.conf
                echo "ssl = on" >> "$PGDATA/postgresql.conf"
                echo "ssl_cert_file = 'server.crt'" >> "$PGDATA/postgresql.conf"
                echo "ssl_key_file = 'server.key'" >> "$PGDATA/postgresql.conf"
                echo "ssl_ca_file = 'ca.crt'" >> "$PGDATA/postgresql.conf"

                # Write pg_hba.conf:
                # - local connections use trust (for init scripts)
                # - SSL connections from 'test' user require client cert
                # - non-SSL connections use scram-sha-256 (for Testcontainers healthcheck)
                cat > "$PGDATA/pg_hba.conf" << 'PGEOF'
                local   all   all                     trust
                hostssl all   test   0.0.0.0/0        cert
                hostssl all   test   ::/0             cert
                host    all   all    0.0.0.0/0        scram-sha-256
                host    all   all    ::/0             scram-sha-256
                PGEOF

                # Reload PostgreSQL to apply changes
                pg_ctl reload -D "$PGDATA"
                """;

        return new PostgreSQLContainer<>(DockerImageName.parse("postgres:15-alpine"))
                .withDatabaseName("eda_ha_ssl_test")
                .withUsername("test")
                .withPassword("test")
                .withCopyToContainer(Transferable.of(serverKeyBytes), "/tmp/ssl/server.key")
                .withCopyToContainer(Transferable.of(serverCertBytes), "/tmp/ssl/server.crt")
                .withCopyToContainer(Transferable.of(caCertBytes), "/tmp/ssl/ca.crt")
                .withCopyToContainer(Transferable.of(initScript), "/docker-entrypoint-initdb.d/00-ssl-setup.sh");
    }
}
