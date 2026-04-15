package org.drools.ansible.rulebook.integration.ha.tests.db.postgres;

import org.drools.ansible.rulebook.integration.ha.tests.ssl.SSLTestCertificateGenerator;

import org.drools.ansible.rulebook.integration.ha.tests.support.TestUtils;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import org.drools.ansible.rulebook.integration.ha.api.HAStateManager;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManagerFactory;
import org.drools.ansible.rulebook.integration.ha.api.HAUtils;
import org.drools.ansible.rulebook.integration.ha.postgres.PostgreSQLStateManager;
import org.drools.ansible.rulebook.integration.ha.model.MatchingEvent;
import org.drools.ansible.rulebook.integration.ha.model.SessionState;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.PostgreSQLContainer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration test for SSL/mTLS connection to PostgreSQL via HAStateManager.
 * <p>
 * This test starts a PostgreSQL container configured for SSL with client certificate
 * authentication, then verifies that the HA state manager can connect via mTLS
 * and perform basic operations.
 * <p>
 * Run with: {@code mvn test -pl drools-ansible-rulebook-integration-ha/drools-ansible-rulebook-integration-ha-tests
 * -Dtest=HAPostgresSSLTest -Dtest.db.type=postgres}
 */
@EnabledIfSystemProperty(named = "test.db.type", matches = "postgres(ql)?")
class HAPostgresSSLTest {

    private static final String WORKER_NAME = "ssl-worker-1";
    private static final String RULE_SET_NAME = "sslTestRuleset";

    private static PostgreSQLContainer<?> sslPostgres;
    private static SSLTestCertificateGenerator.CertBundle bundle;
    private static Path tempDir;

    @BeforeAll
    static void setUp() throws Exception {
        tempDir = Files.createTempDirectory("ha-ssl-test-");
        bundle = SSLTestCertificateGenerator.generate(tempDir.resolve("certs"));

        sslPostgres = PostgresSSLJdbcTest.createSSLPostgresContainer(bundle);
        sslPostgres.start();
    }

    @AfterAll
    static void tearDown() {
        if (sslPostgres != null && sslPostgres.isRunning()) {
            sslPostgres.stop();
        }
    }

    // Key format: Traditional OpenSSL PEM encryption (Proc-Type / DEK-Info)
    @Test
    void testSSLConnectionWithEncryptedPemKey() throws Exception {
        String haUuid = "ssl-test-encrypted-pem";
        Map<String, Object> dbParams = buildBaseDbParams();
        dbParams.put("sslkey", bundle.clientKey().toString());
        dbParams.put("sslcert", bundle.clientCert().toString());
        dbParams.put("sslpassword", bundle.passphrase());

        HAStateManager stateManager = HAStateManagerFactory.create("postgres");
        try {
            stateManager.initializeHA(haUuid, WORKER_NAME, dbParams, Map.of("write_after", 1));
            stateManager.enableLeader();

            verifyBasicOperations(stateManager, haUuid);
        } finally {
            stateManager.shutdown();
        }
    }

    // Key format: Traditional OpenSSL PEM encryption, varying sslmode
    // pg_hba.conf allows both SSL (cert auth) and non-SSL (scram-sha-256) for user 'test',
    // so all sslmode values should succeed:
    //   disable/allow  -> non-SSL with password auth (client cert params ignored)
    //   prefer/require/verify-ca/verify-full -> SSL with client certificate auth
    @ParameterizedTest(name = "sslmode={0}")
    @ValueSource(strings = {"disable", "allow", "prefer", "require", "verify-ca", "verify-full"})
    void testSSLConnectionWithSslMode(String sslmode) throws Exception {
        String haUuid = "ssl-test-sslmode-" + sslmode;
        Map<String, Object> dbParams = buildBaseDbParams();
        dbParams.put("sslmode", sslmode);
        dbParams.put("sslkey", bundle.clientKey().toString());
        dbParams.put("sslcert", bundle.clientCert().toString());
        dbParams.put("sslpassword", bundle.passphrase());

        HAStateManager stateManager = HAStateManagerFactory.create("postgres");
        try {
            stateManager.initializeHA(haUuid, WORKER_NAME, dbParams, Map.of("write_after", 1));
            stateManager.enableLeader();

            verifyBasicOperations(stateManager, haUuid);
        } finally {
            stateManager.shutdown();
        }
    }

    // Key format: unencrypted PEM PKCS#1
    @Test
    void testSSLConnectionWithUnencryptedPkcs1PemKey() throws Exception {
        SSLTestCertificateGenerator.CertBundle pkcs1Bundle =
                SSLTestCertificateGenerator.withUnencryptedPkcs1PemKey(bundle, tempDir.resolve("client-pkcs1.pem"));

        String haUuid = "ssl-test-unencrypted-pkcs1-pem";
        Map<String, Object> dbParams = buildBaseDbParams();
        dbParams.put("sslkey", pkcs1Bundle.clientKey().toString());
        dbParams.put("sslcert", bundle.clientCert().toString());

        HAStateManager stateManager = HAStateManagerFactory.create("postgres");
        try {
            stateManager.initializeHA(haUuid, WORKER_NAME, dbParams, Map.of("write_after", 1));
            stateManager.enableLeader();

            verifyBasicOperations(stateManager, haUuid);
        } finally {
            stateManager.shutdown();
        }
    }

    // Key format: PKCS#8 encrypted PEM (PBES2)
    @Test
    void testSSLConnectionWithPkcs8EncryptedPemKey() throws Exception {
        SSLTestCertificateGenerator.CertBundle encBundle =
                SSLTestCertificateGenerator.withPkcs8EncryptedPemKey(bundle, tempDir.resolve("client-pkcs8-enc.pem"),
                        SSLTestCertificateGenerator.TEST_PASSPHRASE);

        String haUuid = "ssl-test-pkcs8-encrypted-pem";
        Map<String, Object> dbParams = buildBaseDbParams();
        dbParams.put("sslkey", encBundle.clientKey().toString());
        dbParams.put("sslcert", bundle.clientCert().toString());
        dbParams.put("sslpassword", encBundle.passphrase());

        HAStateManager stateManager = HAStateManagerFactory.create("postgres");
        try {
            stateManager.initializeHA(haUuid, WORKER_NAME, dbParams, Map.of("write_after", 1));
            stateManager.enableLeader();

            verifyBasicOperations(stateManager, haUuid);
        } finally {
            stateManager.shutdown();
        }
    }

    // Key format: unencrypted PEM PKCS#8
    @Test
    void testSSLConnectionWithUnencryptedPkcs8PemKey() throws Exception {
        SSLTestCertificateGenerator.CertBundle pkcs8Bundle =
                SSLTestCertificateGenerator.withUnencryptedPkcs8PemKey(bundle, tempDir.resolve("client-pkcs8.pem"));

        String haUuid = "ssl-test-unencrypted-pkcs8-pem";
        Map<String, Object> dbParams = buildBaseDbParams();
        dbParams.put("sslkey", pkcs8Bundle.clientKey().toString());
        dbParams.put("sslcert", bundle.clientCert().toString());

        HAStateManager stateManager = HAStateManagerFactory.create("postgres");
        try {
            stateManager.initializeHA(haUuid, WORKER_NAME, dbParams, Map.of("write_after", 1));
            stateManager.enableLeader();

            verifyBasicOperations(stateManager, haUuid);
        } finally {
            stateManager.shutdown();
        }
    }

    // Wrong CA: verify-ca and verify-full should reject the untrusted server cert
    @ParameterizedTest(name = "sslmode={0} with wrong CA should fail")
    @ValueSource(strings = {"verify-ca", "verify-full"})
    void testWrongCARejectedByVerifyModes(String sslmode) throws Exception {
        // Generate a second, unrelated CA
        SSLTestCertificateGenerator.CertBundle wrongCaBundle =
                SSLTestCertificateGenerator.generate(tempDir.resolve("wrong-ca-" + sslmode));

        String haUuid = "ssl-test-wrong-ca-" + sslmode;
        Map<String, Object> dbParams = buildBaseDbParams();
        dbParams.put("sslmode", sslmode);
        dbParams.put("sslrootcert", wrongCaBundle.caCert().toString()); // wrong CA
        dbParams.put("sslkey", bundle.clientKey().toString());
        dbParams.put("sslcert", bundle.clientCert().toString());
        dbParams.put("sslpassword", bundle.passphrase());

        HAStateManager stateManager = HAStateManagerFactory.create("postgres");
        assertThatThrownBy(() ->
                stateManager.initializeHA(haUuid, WORKER_NAME, dbParams, Map.of("write_after", 1)))
                .isInstanceOf(RuntimeException.class);
    }

    // Wrong CA: require mode does not verify server cert, so it should still succeed
    @Test
    void testWrongCAAcceptedByRequireMode() throws Exception {
        // Generate a second, unrelated CA
        SSLTestCertificateGenerator.CertBundle wrongCaBundle =
                SSLTestCertificateGenerator.generate(tempDir.resolve("wrong-ca-require"));

        String haUuid = "ssl-test-wrong-ca-require";
        Map<String, Object> dbParams = buildBaseDbParams();
        dbParams.put("sslmode", "require");
        dbParams.put("sslrootcert", wrongCaBundle.caCert().toString()); // wrong CA, but require doesn't check
        dbParams.put("sslkey", bundle.clientKey().toString());
        dbParams.put("sslcert", bundle.clientCert().toString());
        dbParams.put("sslpassword", bundle.passphrase());

        HAStateManager stateManager = HAStateManagerFactory.create("postgres");
        try {
            stateManager.initializeHA(haUuid, WORKER_NAME, dbParams, Map.of("write_after", 1));
            stateManager.enableLeader();

            verifyBasicOperations(stateManager, haUuid);
        } finally {
            stateManager.shutdown();
        }
    }

    // Client certificate CN mismatch: CN=wronguser doesn't match any PostgreSQL role
    @Test
    void testClientCertCNMismatchRejected() throws Exception {
        SSLTestCertificateGenerator.CertBundle wrongCnBundle =
                SSLTestCertificateGenerator.withClientCN(bundle, "wronguser", tempDir.resolve("wrong-cn"));

        String haUuid = "ssl-test-wrong-cn";
        Map<String, Object> dbParams = buildBaseDbParams();
        dbParams.put("sslkey", wrongCnBundle.clientKey().toString());
        dbParams.put("sslcert", wrongCnBundle.clientCert().toString());
        dbParams.put("sslpassword", wrongCnBundle.passphrase());

        HAStateManager stateManager = HAStateManagerFactory.create("postgres");
        assertThatThrownBy(() ->
                stateManager.initializeHA(haUuid, WORKER_NAME, dbParams, Map.of("write_after", 1)))
                .isInstanceOf(RuntimeException.class);
    }

    // PKCS#12 format is not supported — should throw UnsupportedOperationException
    @Test
    void testPkcs12KeyFormatRejected() throws Exception {
        SSLTestCertificateGenerator.CertBundle p12Bundle =
                SSLTestCertificateGenerator.withPkcs12Key(bundle, tempDir.resolve("client-rejected.p12"),
                        SSLTestCertificateGenerator.TEST_PASSPHRASE);

        String haUuid = "ssl-test-p12-rejected";
        Map<String, Object> dbParams = buildBaseDbParams();
        dbParams.put("sslkey", p12Bundle.clientKey().toString());
        dbParams.put("sslcert", bundle.clientCert().toString());
        dbParams.put("sslpassword", p12Bundle.passphrase());

        HAStateManager stateManager = HAStateManagerFactory.create("postgres");
        assertThatThrownBy(() ->
                stateManager.initializeHA(haUuid, WORKER_NAME, dbParams, Map.of("write_after", 1)))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("PKCS#12 format (.p12/.pfx) is not supported");
    }

    // DER format is not supported — should throw UnsupportedOperationException
    @Test
    void testDerKeyFormatRejected() throws Exception {
        SSLTestCertificateGenerator.CertBundle derBundle =
                SSLTestCertificateGenerator.withDerUnencryptedKey(bundle, tempDir.resolve("client-rejected.der"));

        String haUuid = "ssl-test-der-rejected";
        Map<String, Object> dbParams = buildBaseDbParams();
        dbParams.put("sslkey", derBundle.clientKey().toString());
        dbParams.put("sslcert", bundle.clientCert().toString());

        HAStateManager stateManager = HAStateManagerFactory.create("postgres");
        assertThatThrownBy(() ->
                stateManager.initializeHA(haUuid, WORKER_NAME, dbParams, Map.of("write_after", 1)))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("DER format (.pk8/.der) is not supported");
    }

    // Wrong passphrase for the traditional encrypted PEM key
    @Test
    void testWrongPassphraseForEncryptedKey() throws Exception {
        String haUuid = "ssl-test-wrong-passphrase";
        Map<String, Object> dbParams = buildBaseDbParams();
        dbParams.put("sslkey", bundle.clientKey().toString());
        dbParams.put("sslcert", bundle.clientCert().toString());
        dbParams.put("sslpassword", "wrong-passphrase");

        HAStateManager stateManager = HAStateManagerFactory.create("postgres");
        assertThatThrownBy(() ->
                stateManager.initializeHA(haUuid, WORKER_NAME, dbParams, Map.of("write_after", 1)))
                .isInstanceOf(RuntimeException.class);
    }

    // Temp P12 cleanup: after initializeHA() with a PEM key, a temp .p12 file is created.
    // After shutdown(), the temp file should be deleted.
    @Test
    void testTempP12CleanupOnShutdown() throws Exception {
        String haUuid = "ssl-test-p12-cleanup";
        Map<String, Object> dbParams = buildBaseDbParams();
        dbParams.put("sslkey", bundle.clientKey().toString());
        dbParams.put("sslcert", bundle.clientCert().toString());
        dbParams.put("sslpassword", bundle.passphrase());

        PostgreSQLStateManager stateManager = (PostgreSQLStateManager) HAStateManagerFactory.create("postgres");
        stateManager.initializeHA(haUuid, WORKER_NAME, dbParams, Map.of("write_after", 1));

        Path tempP12 = stateManager.getTempP12KeystorePath();
        assertThat(tempP12).isNotNull();
        assertThat(tempP12).exists();

        stateManager.shutdown();

        assertThat(tempP12).doesNotExist();
        assertThat(tempP12.getParent()).doesNotExist();
        assertThat(stateManager.getTempP12KeystorePath()).isNull();
    }

    // Missing client certificate: sslmode=require without sslkey/sslcert.
    // pg_hba.conf's "hostssl all test ... cert" rule requires a client cert,
    // so the connection is rejected.
    @Test
    void testMissingClientCertificateRejected() throws Exception {
        String haUuid = "ssl-test-no-client-cert";
        Map<String, Object> dbParams = buildBaseDbParams();
        dbParams.put("sslmode", "require");
        // Deliberately omit sslkey and sslcert

        HAStateManager stateManager = HAStateManagerFactory.create("postgres");
        assertThatThrownBy(() ->
                stateManager.initializeHA(haUuid, WORKER_NAME, dbParams, Map.of("write_after", 1)))
                .isInstanceOf(RuntimeException.class);
    }

    private Map<String, Object> buildBaseDbParams() {
        Map<String, Object> dbParams = new HashMap<>();
        dbParams.put("db_type", "postgres");
        dbParams.put("host", sslPostgres.getHost());
        dbParams.put("port", sslPostgres.getMappedPort(5432));
        dbParams.put("database", sslPostgres.getDatabaseName());
        dbParams.put("user", sslPostgres.getUsername());
        dbParams.put("password", sslPostgres.getPassword());
        dbParams.put("sslmode", "verify-full");
        dbParams.put("sslrootcert", bundle.caCert().toString());
        return dbParams;
    }

    private void verifyBasicOperations(HAStateManager stateManager, String haUuid) {
        SessionState sessionState = new SessionState();
        sessionState.setHaUuid(haUuid);
        sessionState.setRuleSetName(RULE_SET_NAME);
        sessionState.setRulebookHash("rulebook-hash-ssl-test");
        long now = System.currentTimeMillis();
        sessionState.setCreatedTime(now);
        sessionState.setPersistedTime(now);
        sessionState.setCurrentStateSHA(HAUtils.calculateStateSHA(sessionState));
        stateManager.persistSessionState(sessionState);

        SessionState retrieved = stateManager.getPersistedSessionState(RULE_SET_NAME);
        assertThat(retrieved).isNotNull();
        assertThat(retrieved.getHaUuid()).isEqualTo(haUuid);
        assertThat(retrieved.getRuleSetName()).isEqualTo(RULE_SET_NAME);

        MatchingEvent me = TestUtils.createMatchingEvent(haUuid, RULE_SET_NAME, "testRule", Map.of("key", "value"));
        String meUuid = stateManager.addMatchingEvent(me);
        assertThat(meUuid).isNotNull();
        assertThat(stateManager.getPendingMatchingEvents()).hasSize(1);

        assertThat(stateManager.getHAStats()).isNotNull();
    }
}
