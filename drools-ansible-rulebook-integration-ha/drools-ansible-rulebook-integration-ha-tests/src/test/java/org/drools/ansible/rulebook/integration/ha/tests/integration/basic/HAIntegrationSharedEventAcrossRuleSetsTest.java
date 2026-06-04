package org.drools.ansible.rulebook.integration.ha.tests.integration.basic;

import java.util.List;
import java.util.Map;

import org.drools.ansible.rulebook.integration.api.RuleConfigurationOption;
import org.drools.ansible.rulebook.integration.core.jpy.AstRulesEngine;
import org.drools.ansible.rulebook.integration.ha.tests.integration.HAIntegrationTestBase;
import org.drools.ansible.rulebook.integration.ha.tests.support.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.readValueAsListOfMapOfStringAndObject;

/**
 * Verifies that the same event sent to two different rule sets is stored
 * independently in drools_ansible_event_record (composite PK: ha_uuid, rule_set_name, record_identifier).
 */
class HAIntegrationSharedEventAcrossRuleSetsTest extends HAIntegrationTestBase {

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

    private long sessionIdAlpha;
    private long sessionIdBeta;

    @Override
    protected String getRuleSet() {
        return RULE_SET_ALPHA;
    }

    @BeforeEach
    @Override
    protected void setUp() {
        System.out.println("Running test with database: " + TEST_DB_TYPE);

        rulesEngine1 = new AstRulesEngine();
        consumer1 = new AsyncConsumer("consumer1");
        consumer1.startConsuming(rulesEngine1.port());
        rulesEngine1.initializeHA(HA_UUID, "worker-1", dbParamsJson, dbHAConfigJson);

        sessionIdAlpha = rulesEngine1.createRuleset(RULE_SET_ALPHA, RuleConfigurationOption.FULLY_MANUAL_PSEUDOCLOCK);
        sessionIdBeta = rulesEngine1.createRuleset(RULE_SET_BETA, RuleConfigurationOption.FULLY_MANUAL_PSEUDOCLOCK);

        sessionId1 = sessionIdAlpha;
    }

    @AfterEach
    @Override
    protected void tearDown() {
        if (consumer1 != null) {
            consumer1.stop();
        }
        if (rulesEngine1 != null) {
            rulesEngine1.dispose(sessionIdAlpha);
            rulesEngine1.dispose(sessionIdBeta);
            rulesEngine1.close();
        }
        cleanupDatabase();
    }

    @Test
    void sameEventPersistedIndependentlyPerRuleSet() {
        rulesEngine1.enableLeader();

        String sharedEvent = "{\"meta\":{\"uuid\":\"shared-uuid-1234\"}, \"i\":1}";

        String resultAlpha = rulesEngine1.assertEvent(sessionIdAlpha, sharedEvent);
        assertThat(readValueAsListOfMapOfStringAndObject(resultAlpha)).isEmpty();

        String resultBeta = rulesEngine1.assertEvent(sessionIdBeta, sharedEvent);
        assertThat(readValueAsListOfMapOfStringAndObject(resultBeta)).isEmpty();

        String totalRows = TestUtils.queryRawColumn(dbParams,
                "SELECT COUNT(*) FROM drools_ansible_event_record WHERE ha_uuid = ?",
                HA_UUID);
        assertThat(totalRows).isEqualTo("2");

        String alphaRows = TestUtils.queryRawColumn(dbParams,
                "SELECT COUNT(*) FROM drools_ansible_event_record "
                        + "WHERE ha_uuid = ? AND rule_set_name = 'Alpha Ruleset'",
                HA_UUID);
        assertThat(alphaRows).isEqualTo("1");

        String betaRows = TestUtils.queryRawColumn(dbParams,
                "SELECT COUNT(*) FROM drools_ansible_event_record "
                        + "WHERE ha_uuid = ? AND rule_set_name = 'Beta Ruleset'",
                HA_UUID);
        assertThat(betaRows).isEqualTo("1");

        String alphaEventJson = TestUtils.queryRawColumn(dbParams,
                "SELECT event_json FROM drools_ansible_event_record "
                        + "WHERE ha_uuid = ? AND rule_set_name = 'Alpha Ruleset'",
                HA_UUID);
        assertThat(alphaEventJson).contains("\"i\":1");

        String betaEventJson = TestUtils.queryRawColumn(dbParams,
                "SELECT event_json FROM drools_ansible_event_record "
                        + "WHERE ha_uuid = ? AND rule_set_name = 'Beta Ruleset'",
                HA_UUID);
        assertThat(betaEventJson).contains("\"i\":1");

        // Complete Alpha by sending the second condition — only Alpha matches
        String secondEventAlpha = TestUtils.createEvent("{\"j\":2}");
        String matchResult = rulesEngine1.assertEvent(sessionIdAlpha, secondEventAlpha);
        List<Map<String, Object>> matches = readValueAsListOfMapOfStringAndObject(matchResult);
        assertThat(matches).hasSize(1);
        assertThat(matches.get(0)).containsEntry("name", "alpha_alert");

        // Beta still holds its partial event
        String betaRowsAfterMatch = TestUtils.queryRawColumn(dbParams,
                "SELECT COUNT(*) FROM drools_ansible_event_record "
                        + "WHERE ha_uuid = ? AND rule_set_name = 'Beta Ruleset'",
                HA_UUID);
        assertThat(betaRowsAfterMatch).isEqualTo("1");
    }
}
