package org.drools.ansible.rulebook.integration.ha.tests.integration.basic;

import java.util.List;
import java.util.Map;

import org.drools.ansible.rulebook.integration.api.RuleConfigurationOption;
import org.drools.ansible.rulebook.integration.core.jpy.AstRulesEngine;
import org.drools.ansible.rulebook.integration.ha.tests.integration.HAIntegrationTestBase;
import org.drools.ansible.rulebook.integration.ha.tests.support.AbstractHATestBase;
import org.drools.ansible.rulebook.integration.ha.tests.support.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.readValueAsListOfMapOfStringAndObject;

/**
 * Verifies that the same event sent to 4 sessions (2 HA_UUIDs x 2 rule sets each)
 * produces 4 independent rows in drools_ansible_event_record, one per (ha_uuid, rule_set_name) pair.
 */
class HAIntegrationMultiHaUuidSharedEventTest extends AbstractHATestBase {

    private static final String HA_UUID_1 = "multi-ha-uuid-1";
    private static final String HA_UUID_2 = "multi-ha-uuid-2";

    private static final String RULE_SET_ALPHA = """
            {
                "name": "Alpha Ruleset",
                "rules": [
                    {"Rule": {
                        "name": "alpha_alert",
                        "condition": {
                            "AllCondition": [
                                {"EqualsExpression": {"lhs": {"Event": "i"}, "rhs": {"Integer": 1}}},
                                {"EqualsExpression": {"lhs": {"Event": "j"}, "rhs": {"Integer": 2}}}
                            ]
                        },
                        "action": {"run_playbook": [{"name": "alpha.yml"}]}
                    }}
                ]
            }
            """;

    private static final String RULE_SET_BETA = """
            {
                "name": "Beta Ruleset",
                "rules": [
                    {"Rule": {
                        "name": "beta_alert",
                        "condition": {
                            "AllCondition": [
                                {"EqualsExpression": {"lhs": {"Event": "i"}, "rhs": {"Integer": 1}}},
                                {"EqualsExpression": {"lhs": {"Event": "k"}, "rhs": {"Integer": 3}}}
                            ]
                        },
                        "action": {"run_playbook": [{"name": "beta.yml"}]}
                    }}
                ]
            }
            """;

    private static final String RULE_SET_GAMMA = """
            {
                "name": "Gamma Ruleset",
                "rules": [
                    {"Rule": {
                        "name": "gamma_alert",
                        "condition": {
                            "AllCondition": [
                                {"EqualsExpression": {"lhs": {"Event": "i"}, "rhs": {"Integer": 1}}},
                                {"EqualsExpression": {"lhs": {"Event": "m"}, "rhs": {"Integer": 4}}}
                            ]
                        },
                        "action": {"run_playbook": [{"name": "gamma.yml"}]}
                    }}
                ]
            }
            """;

    private static final String RULE_SET_DELTA = """
            {
                "name": "Delta Ruleset",
                "rules": [
                    {"Rule": {
                        "name": "delta_alert",
                        "condition": {
                            "AllCondition": [
                                {"EqualsExpression": {"lhs": {"Event": "i"}, "rhs": {"Integer": 1}}},
                                {"EqualsExpression": {"lhs": {"Event": "n"}, "rhs": {"Integer": 5}}}
                            ]
                        },
                        "action": {"run_playbook": [{"name": "delta.yml"}]}
                    }}
                ]
            }
            """;

    static {
        if (USE_POSTGRES) {
            initializePostgres("eda_ha_multi_uuid_event_test", "Multi HA-UUID shared event tests");
        } else {
            initializeH2();
        }
    }

    private AstRulesEngine engine1;
    private AstRulesEngine engine2;

    private HAIntegrationTestBase.AsyncConsumer consumer1;
    private HAIntegrationTestBase.AsyncConsumer consumer2;

    private long sessionAlpha1;
    private long sessionBeta1;
    private long sessionGamma2;
    private long sessionDelta2;

    @BeforeEach
    void setUp() {
        engine1 = new AstRulesEngine();
        consumer1 = new HAIntegrationTestBase.AsyncConsumer("consumer1");
        consumer1.startConsuming(engine1.port());
        engine1.initializeHA(HA_UUID_1, "worker-1", dbParamsJson, dbHAConfigJson);
        sessionAlpha1 = engine1.createRuleset(RULE_SET_ALPHA, RuleConfigurationOption.FULLY_MANUAL_PSEUDOCLOCK);
        sessionBeta1 = engine1.createRuleset(RULE_SET_BETA, RuleConfigurationOption.FULLY_MANUAL_PSEUDOCLOCK);

        engine2 = new AstRulesEngine();
        consumer2 = new HAIntegrationTestBase.AsyncConsumer("consumer2");
        consumer2.startConsuming(engine2.port());
        engine2.initializeHA(HA_UUID_2, "worker-2", dbParamsJson, dbHAConfigJson);
        sessionGamma2 = engine2.createRuleset(RULE_SET_GAMMA, RuleConfigurationOption.FULLY_MANUAL_PSEUDOCLOCK);
        sessionDelta2 = engine2.createRuleset(RULE_SET_DELTA, RuleConfigurationOption.FULLY_MANUAL_PSEUDOCLOCK);
    }

    @AfterEach
    void tearDown() {
        if (consumer1 != null) consumer1.stop();
        if (consumer2 != null) consumer2.stop();

        if (engine1 != null) {
            try { engine1.dispose(sessionAlpha1); } catch (Exception ignored) {}
            try { engine1.dispose(sessionBeta1); } catch (Exception ignored) {}
            engine1.close();
        }
        if (engine2 != null) {
            try { engine2.dispose(sessionGamma2); } catch (Exception ignored) {}
            try { engine2.dispose(sessionDelta2); } catch (Exception ignored) {}
            engine2.close();
        }
        cleanupDatabase();
    }

    @Test
    void sameEventStoredAcrossTwoHaUuidsAndFourRuleSets() {
        engine1.enableLeader();
        engine2.enableLeader();

        String sharedEvent = "{\"meta\":{\"uuid\":\"shared-across-clusters\"}, \"i\":1}";

        assertThat(readValueAsListOfMapOfStringAndObject(engine1.assertEvent(sessionAlpha1, sharedEvent))).isEmpty();
        assertThat(readValueAsListOfMapOfStringAndObject(engine1.assertEvent(sessionBeta1, sharedEvent))).isEmpty();
        assertThat(readValueAsListOfMapOfStringAndObject(engine2.assertEvent(sessionGamma2, sharedEvent))).isEmpty();
        assertThat(readValueAsListOfMapOfStringAndObject(engine2.assertEvent(sessionDelta2, sharedEvent))).isEmpty();

        // 4 rows total across both HA UUIDs
        String ha1Rows = TestUtils.queryRawColumn(dbParams,
                "SELECT COUNT(*) FROM drools_ansible_event_record WHERE ha_uuid = ?",
                HA_UUID_1);
        assertThat(ha1Rows).isEqualTo("2");

        String ha2Rows = TestUtils.queryRawColumn(dbParams,
                "SELECT COUNT(*) FROM drools_ansible_event_record WHERE ha_uuid = ?",
                HA_UUID_2);
        assertThat(ha2Rows).isEqualTo("2");

        // 1 row per (ha_uuid, rule_set_name) pair
        String alphaRows = TestUtils.queryRawColumn(dbParams,
                "SELECT COUNT(*) FROM drools_ansible_event_record "
                        + "WHERE ha_uuid = ? AND rule_set_name = 'Alpha Ruleset'",
                HA_UUID_1);
        assertThat(alphaRows).isEqualTo("1");

        String betaRows = TestUtils.queryRawColumn(dbParams,
                "SELECT COUNT(*) FROM drools_ansible_event_record "
                        + "WHERE ha_uuid = ? AND rule_set_name = 'Beta Ruleset'",
                HA_UUID_1);
        assertThat(betaRows).isEqualTo("1");

        String gammaRows = TestUtils.queryRawColumn(dbParams,
                "SELECT COUNT(*) FROM drools_ansible_event_record "
                        + "WHERE ha_uuid = ? AND rule_set_name = 'Gamma Ruleset'",
                HA_UUID_2);
        assertThat(gammaRows).isEqualTo("1");

        String deltaRows = TestUtils.queryRawColumn(dbParams,
                "SELECT COUNT(*) FROM drools_ansible_event_record "
                        + "WHERE ha_uuid = ? AND rule_set_name = 'Delta Ruleset'",
                HA_UUID_2);
        assertThat(deltaRows).isEqualTo("1");

        // All 4 rows contain the same event payload
        String alphaJson = TestUtils.queryRawColumn(dbParams,
                "SELECT event_json FROM drools_ansible_event_record "
                        + "WHERE ha_uuid = ? AND rule_set_name = 'Alpha Ruleset'",
                HA_UUID_1);
        String betaJson = TestUtils.queryRawColumn(dbParams,
                "SELECT event_json FROM drools_ansible_event_record "
                        + "WHERE ha_uuid = ? AND rule_set_name = 'Beta Ruleset'",
                HA_UUID_1);
        String gammaJson = TestUtils.queryRawColumn(dbParams,
                "SELECT event_json FROM drools_ansible_event_record "
                        + "WHERE ha_uuid = ? AND rule_set_name = 'Gamma Ruleset'",
                HA_UUID_2);
        String deltaJson = TestUtils.queryRawColumn(dbParams,
                "SELECT event_json FROM drools_ansible_event_record "
                        + "WHERE ha_uuid = ? AND rule_set_name = 'Delta Ruleset'",
                HA_UUID_2);
        assertThat(alphaJson).contains("\"i\":1");
        assertThat(betaJson).contains("\"i\":1");
        assertThat(gammaJson).contains("\"i\":1");
        assertThat(deltaJson).contains("\"i\":1");

        // Complete Alpha (j=2) and Gamma (m=4) — Beta and Delta remain partial
        String matchAlpha = rulesEngine1MatchEvent(sessionAlpha1, "{\"j\":2}");
        List<Map<String, Object>> alphaMatches = readValueAsListOfMapOfStringAndObject(matchAlpha);
        assertThat(alphaMatches).hasSize(1);
        assertThat(alphaMatches.get(0)).containsEntry("name", "alpha_alert");

        String matchGamma = rulesEngine2MatchEvent(sessionGamma2, "{\"m\":4}");
        List<Map<String, Object>> gammaMatches = readValueAsListOfMapOfStringAndObject(matchGamma);
        assertThat(gammaMatches).hasSize(1);
        assertThat(gammaMatches.get(0)).containsEntry("name", "gamma_alert");

        // Beta and Delta still hold their partial events
        String betaRowsAfter = TestUtils.queryRawColumn(dbParams,
                "SELECT COUNT(*) FROM drools_ansible_event_record "
                        + "WHERE ha_uuid = ? AND rule_set_name = 'Beta Ruleset'",
                HA_UUID_1);
        assertThat(betaRowsAfter).isEqualTo("1");

        String deltaRowsAfter = TestUtils.queryRawColumn(dbParams,
                "SELECT COUNT(*) FROM drools_ansible_event_record "
                        + "WHERE ha_uuid = ? AND rule_set_name = 'Delta Ruleset'",
                HA_UUID_2);
        assertThat(deltaRowsAfter).isEqualTo("1");
    }

    private String rulesEngine1MatchEvent(long sessionId, String eventBody) {
        return engine1.assertEvent(sessionId, TestUtils.createEvent(eventBody));
    }

    private String rulesEngine2MatchEvent(long sessionId, String eventBody) {
        return engine2.assertEvent(sessionId, TestUtils.createEvent(eventBody));
    }
}
