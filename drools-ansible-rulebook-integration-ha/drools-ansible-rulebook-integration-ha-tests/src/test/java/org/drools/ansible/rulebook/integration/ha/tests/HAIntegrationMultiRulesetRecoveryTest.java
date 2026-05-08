package org.drools.ansible.rulebook.integration.ha.tests;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.drools.ansible.rulebook.integration.api.RuleConfigurationOption;
import org.drools.ansible.rulebook.integration.api.io.JsonMapper;
import org.drools.ansible.rulebook.integration.core.jpy.AstRulesEngine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.readValueAsMapOfStringAndObject;
import static org.drools.ansible.rulebook.integration.ha.tests.TestUtils.createEvent;

/**
 * Tests that ME recovery on failover sends the correct session_id for each ruleset.
 *
 * Before the fix, recoverPendingMatchingEvents() looped over all executors and sent ALL
 * pending MEs with each executor's session_id. This caused:
 *   1. Duplicate MEs sent through the async channel
 *   2. MEs tagged with the wrong session_id (e.g. "Humidity Ruleset" MEs sent with the
 *      "Temperature Ruleset" session_id)
 *
 * After the fix, MEs are grouped by ruleSetName and each executor only receives its own MEs.
 */
class HAIntegrationMultiRulesetRecoveryTest extends AbstractHATestBase {

    private static final String HA_UUID = "multi-ruleset-recovery-ha-1";

    private static final String RULE_SET_TEMPERATURE = """
            {
                "name": "Temperature Ruleset",
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
                            "run_playbook": [{"name": "temp_alert.yml"}]
                        }
                    }}
                ]
            }
            """;

    private static final String RULE_SET_HUMIDITY = """
            {
                "name": "Humidity Ruleset",
                "sources": {"EventSource": "test"},
                "rules": [
                    {"Rule": {
                        "name": "humidity_alert",
                        "condition": {
                            "GreaterThanExpression": {
                                "lhs": {"Event": "humidity"},
                                "rhs": {"Integer": 70}
                            }
                        },
                        "action": {
                            "run_playbook": [{"name": "humid_alert.yml"}]
                        }
                    }}
                ]
            }
            """;

    static {
        if (USE_POSTGRES) {
            initializePostgres("eda_ha_multi_rs_test", "Multi-ruleset recovery tests");
        } else {
            initializeH2();
        }
    }

    private AstRulesEngine rulesEngine1;
    private AstRulesEngine rulesEngine2;

    private long sessionIdTemp1;
    private long sessionIdHumid1;
    private long sessionIdTemp2;
    private long sessionIdHumid2;

    private HAIntegrationTestBase.AsyncConsumer consumer1;
    private HAIntegrationTestBase.AsyncConsumer consumer2;

    @BeforeEach
    void setUp() {
        // Node 1 with two rulesets
        rulesEngine1 = new AstRulesEngine();
        consumer1 = new HAIntegrationTestBase.AsyncConsumer("consumer1");
        consumer1.startConsuming(rulesEngine1.port());
        rulesEngine1.initializeHA(HA_UUID, "worker-1", dbParamsJson, dbHAConfigJson);
        sessionIdTemp1 = rulesEngine1.createRuleset(RULE_SET_TEMPERATURE, RuleConfigurationOption.FULLY_MANUAL_PSEUDOCLOCK);
        sessionIdHumid1 = rulesEngine1.createRuleset(RULE_SET_HUMIDITY, RuleConfigurationOption.FULLY_MANUAL_PSEUDOCLOCK);

        // Node 2 with the same two rulesets (prepared before failover)
        rulesEngine2 = new AstRulesEngine();
        consumer2 = new HAIntegrationTestBase.AsyncConsumer("consumer2");
        consumer2.startConsuming(rulesEngine2.port());
        rulesEngine2.initializeHA(HA_UUID, "worker-2", dbParamsJson, dbHAConfigJson);
        sessionIdTemp2 = rulesEngine2.createRuleset(RULE_SET_TEMPERATURE, RuleConfigurationOption.FULLY_MANUAL_PSEUDOCLOCK);
        sessionIdHumid2 = rulesEngine2.createRuleset(RULE_SET_HUMIDITY, RuleConfigurationOption.FULLY_MANUAL_PSEUDOCLOCK);
    }

    @AfterEach
    void tearDown() {
        if (consumer1 != null) consumer1.stop();
        if (consumer2 != null) consumer2.stop();

        if (rulesEngine1 != null) {
            try { rulesEngine1.dispose(sessionIdTemp1); } catch (Exception ignored) {}
            try { rulesEngine1.dispose(sessionIdHumid1); } catch (Exception ignored) {}
            rulesEngine1.close();
        }
        if (rulesEngine2 != null) {
            try { rulesEngine2.dispose(sessionIdTemp2); } catch (Exception ignored) {}
            try { rulesEngine2.dispose(sessionIdHumid2); } catch (Exception ignored) {}
            rulesEngine2.close();
        }

        cleanupDatabase();
    }

    /**
     * Verifies that after failover, each ruleset's pending MEs are sent with the correct session_id.
     *
     * Scenario:
     *   1. Node1 is leader with two rulesets (Temperature, Humidity)
     *   2. Fire one event per ruleset to create one ME each
     *   3. Node1 crashes, Node2 becomes leader
     *   4. Verify async recovery messages carry the correct session_id for each ruleset
     */
    @Test
    void testMultiRulesetRecoverySendsCorrectSessionId() {
        rulesEngine1.enableLeader();

        // Fire a temperature event -> ME for "Temperature Ruleset"
        String tempResult = rulesEngine1.assertEvent(sessionIdTemp1,
                createEvent("{\"temperature\": 45}"));
        String tempMeUuid = TestUtils.extractMatchingUuidFromResponse(tempResult);
        assertThat(tempMeUuid).isNotNull();

        // Fire a humidity event -> ME for "Humidity Ruleset"
        String humidResult = rulesEngine1.assertEvent(sessionIdHumid1,
                createEvent("{\"humidity\": 85}"));
        String humidMeUuid = TestUtils.extractMatchingUuidFromResponse(humidResult);
        assertThat(humidMeUuid).isNotNull();

        // Crash node1
        rulesEngine1.disableLeader();
        rulesEngine1.close();
        rulesEngine1 = null;
        consumer1.stop();

        // Failover: node2 becomes leader -> triggers recoverPendingMatchingEvents
        rulesEngine2.enableLeader();

        // Wait for async recovery messages (one per ruleset, so 2 messages expected)
        await().atMost(5, TimeUnit.SECONDS)
                .until(() -> consumer2.getReceivedMessages().size() >= 2);

        // Exactly 2 messages: one for each ruleset
        assertThat(consumer2.getReceivedMessages()).hasSize(2);

        // Parse both messages
        Map<String, Object> msg1 = readValueAsMapOfStringAndObject(consumer2.getReceivedMessages().get(0));
        Map<String, Object> msg2 = readValueAsMapOfStringAndObject(consumer2.getReceivedMessages().get(1));

        // Identify which message is for which ruleset
        Map<String, Object> tempMsg;
        Map<String, Object> humidMsg;
        if (containsRuleset(msg1, "Temperature Ruleset")) {
            tempMsg = msg1;
            humidMsg = msg2;
        } else {
            tempMsg = msg2;
            humidMsg = msg1;
        }

        // Verify Temperature Ruleset recovery message
        List<Map<String, Object>> tempResults = (List<Map<String, Object>>) tempMsg.get("result");
        assertThat(tempResults).hasSize(1);
        assertThat(tempResults.get(0)).containsEntry("matching_uuid", tempMeUuid);
        assertThat(tempResults.get(0)).containsEntry("ruleset_name", "Temperature Ruleset");
        assertThat(tempResults.get(0)).containsEntry("name", "temperature_alert");
        // session_id must match the Temperature Ruleset's sessionId on node2
        assertThat(((Number) tempMsg.get("session_id")).longValue()).isEqualTo(sessionIdTemp2);

        // Verify Humidity Ruleset recovery message
        List<Map<String, Object>> humidResults = (List<Map<String, Object>>) humidMsg.get("result");
        assertThat(humidResults).hasSize(1);
        assertThat(humidResults.get(0)).containsEntry("matching_uuid", humidMeUuid);
        assertThat(humidResults.get(0)).containsEntry("ruleset_name", "Humidity Ruleset");
        assertThat(humidResults.get(0)).containsEntry("name", "humidity_alert");
        // session_id must match the Humidity Ruleset's sessionId on node2
        assertThat(((Number) humidMsg.get("session_id")).longValue()).isEqualTo(sessionIdHumid2);
    }

    @SuppressWarnings("unchecked")
    private boolean containsRuleset(Map<String, Object> msg, String rulesetName) {
        List<Map<String, Object>> results = (List<Map<String, Object>>) msg.get("result");
        return results != null && results.stream()
                .anyMatch(r -> rulesetName.equals(r.get("ruleset_name")));
    }
}
