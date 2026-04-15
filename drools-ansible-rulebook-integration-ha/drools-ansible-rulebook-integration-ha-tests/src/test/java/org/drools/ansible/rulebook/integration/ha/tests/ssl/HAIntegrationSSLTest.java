package org.drools.ansible.rulebook.integration.ha.tests.ssl;

import org.drools.ansible.rulebook.integration.ha.tests.db.postgres.PostgresSSLJdbcTest;

import org.drools.ansible.rulebook.integration.ha.tests.support.TestUtils;

import org.drools.ansible.rulebook.integration.ha.tests.integration.HAIntegrationTestBase;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.drools.ansible.rulebook.integration.api.io.JsonMapper;
import org.drools.ansible.rulebook.integration.core.jpy.AstRulesEngine;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.testcontainers.containers.PostgreSQLContainer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.toJson;

/**
 * Integration test for AstRulesEngine with HA over SSL/mTLS PostgreSQL.
 * <p>
 * Verifies the full flow: AstRulesEngine → HA initialization → SSL connection
 * (with PEM-to-P12 conversion) → rule evaluation → matching event persistence.
 * <p>
 * Run with: {@code mvn test -pl drools-ansible-rulebook-integration-ha/drools-ansible-rulebook-integration-ha-tests
 * -Dtest=HAIntegrationSSLTest -Dtest.db.type=postgres}
 */
@EnabledIfSystemProperty(named = "test.db.type", matches = "postgres(ql)?")
class HAIntegrationSSLTest {

    private static final String HA_UUID = "integration-ha-ssl-1";

    private static final String RULE_SET = """
                {
                    "name": "SSL Test Ruleset",
                    "sources": {"EventSource": "test"},
                    "rules": [
                        {"Rule": {
                            "name": "temperature_alert",
                            "condition": {
                                "GreaterThanExpression": {
                                    "lhs": {"Event": "temperature"},
                                    "rhs": {"Integer": 30}
                                }
                            },
                            "action": {
                                "run_playbook": [{"name": "alert.yml"}]
                            }
                        }}
                    ]
                }
                """;

    private static PostgreSQLContainer<?> sslPostgres;
    private static SSLTestCertificateGenerator.CertBundle bundle;
    private static Path tempDir;
    private static String dbParamsJson;
    private static String dbHAConfigJson;

    private AstRulesEngine rulesEngine;
    private HAIntegrationTestBase.AsyncConsumer consumer;
    private long sessionId;

    @BeforeAll
    static void setUpSSLPostgres() throws Exception {
        tempDir = Files.createTempDirectory("ha-engine-ssl-test-");
        bundle = SSLTestCertificateGenerator.generate(tempDir.resolve("certs"));
        sslPostgres = PostgresSSLJdbcTest.createSSLPostgresContainer(bundle);
        sslPostgres.start();

        Map<String, Object> dbParams = buildDbParams();
        dbParamsJson = toJson(dbParams);
        dbHAConfigJson = toJson(Map.of("write_after", 1));

        // For TestUtils.dropPostgresTables() cleanup (uses plain JDBC, no SSL needed)
        TestUtils.setDbTestConfig(dbParams, Map.of("write_after", 1));
    }

    @AfterAll
    static void tearDownSSLPostgres() {
        if (sslPostgres != null && sslPostgres.isRunning()) {
            sslPostgres.stop();
        }
    }

    @BeforeEach
    void setUp() {
        TestUtils.dropPostgresTables();

        rulesEngine = new AstRulesEngine();

        consumer = new HAIntegrationTestBase.AsyncConsumer("ssl-consumer");
        consumer.startConsuming(rulesEngine.port());

        rulesEngine.initializeHA(HA_UUID, "worker-1", dbParamsJson, dbHAConfigJson);
        sessionId = rulesEngine.createRuleset(RULE_SET);
    }

    @AfterEach
    void tearDown() {
        if (consumer != null) {
            consumer.stop();
        }
        if (rulesEngine != null) {
            rulesEngine.dispose(sessionId);
            rulesEngine.close();
        }
    }

    @Test
    void testBasicRuleWithSSLPostgres() {
        rulesEngine.enableLeader();

        String event = TestUtils.createEvent("{\"temperature\": 35}");
        String result = rulesEngine.assertEvent(sessionId, event);

        assertThat(result).isNotNull();

        List<Map<String, Object>> matchList = JsonMapper.readValueAsListOfMapOfStringAndObject(result);
        assertThat(matchList).hasSize(1);
        assertThat(matchList.get(0).get("name")).isEqualTo("temperature_alert");
        assertThat(matchList.get(0).get("matching_uuid")).isNotNull();

        // Verify session stats
        String stats = rulesEngine.sessionStats(sessionId);
        Map<String, Object> statsMap = JsonMapper.readValueAsMapOfStringAndObject(stats);
        assertThat(statsMap.get("rulesTriggered")).isEqualTo(1);
    }

    private static Map<String, Object> buildDbParams() {
        Map<String, Object> params = new HashMap<>();
        params.put("db_type", "postgres");
        params.put("host", sslPostgres.getHost());
        params.put("port", sslPostgres.getMappedPort(5432));
        params.put("database", sslPostgres.getDatabaseName());
        params.put("user", sslPostgres.getUsername());
        params.put("password", sslPostgres.getPassword());
        params.put("sslmode", "verify-full");
        params.put("sslrootcert", bundle.caCert().toString());
        params.put("sslcert", bundle.clientCert().toString());
        params.put("sslkey", bundle.clientKey().toString());
        params.put("sslpassword", bundle.passphrase());
        return params;
    }
}
