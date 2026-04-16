package org.drools.ansible.rulebook.integration.ha.tests.integration.memory;

import java.io.IOException;
import java.net.Socket;
import java.util.List;
import java.util.Map;

import org.drools.ansible.rulebook.integration.api.RuleConfigurationOption;
import org.drools.ansible.rulebook.integration.api.io.JsonMapper;
import org.drools.ansible.rulebook.integration.core.jpy.AstRulesEngine;
import org.drools.ansible.rulebook.integration.ha.tests.support.AbstractHATestBase;
import org.drools.ansible.rulebook.integration.ha.tests.support.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.readValueAsListOfMapOfStringAndObject;
import static org.drools.ansible.rulebook.integration.ha.tests.support.TestUtils.createEvent;
import static org.drools.ansible.rulebook.integration.ha.tests.support.TestUtils.extractMatchingUuidFromResponse;

class HAIntegrationMemoryLeakTest extends AbstractHATestBase {

    private static final String HA_UUID = "memory-leak-ha";
    private static final int EVENT_COUNT = 2000;
    private static final int BLOB_SIZE = 90 * 1024;

    private static final String RULE_SET = """
            {
                "name": "Memory Leak Ruleset",
                "sources": {"EventSource": "test"},
                "rules": [
                    {"Rule": {
                        "name": "always_fire",
                        "condition": {
                            "AllCondition": [
                                {
                                    "EqualsExpression": {
                                        "lhs": {
                                            "Event": "trigger"
                                        },
                                        "rhs": {
                                            "Boolean": true
                                        }
                                    }
                                }
                            ]
                        },
                        "action": {
                            "run_playbook": [
                                {
                                    "name": "no_action.yml"
                                }
                            ]
                        }
                    }}
                ]
            }
            """;

    static {
        // Set the log level to WARN to avoid excessive logging during tests, when running on IDE
        System.setProperty("org.slf4j.simpleLogger.log.org.drools.ansible.rulebook.integration", "WARN");

        if (USE_POSTGRES) {
            initializePostgres("eda_ha_test", "HA memory leak tests");
        } else {
            initializeH2();
        }
    }

    private AstRulesEngine rulesEngine;
    private long sessionId;
    private Socket asyncClientSocket;

    @BeforeEach
    void setUp() throws IOException {
        rulesEngine = new AstRulesEngine();

        // HA leader startup requires an async client connection even though this test exercises
        // the synchronous assertEvent path.
        asyncClientSocket = new Socket("localhost", rulesEngine.port());

        rulesEngine.initializeHA(HA_UUID, "worker-1", dbParamsJson, dbHAConfigJson);
        sessionId = rulesEngine.createRuleset(RULE_SET, RuleConfigurationOption.FULLY_MANUAL_PSEUDOCLOCK);
        rulesEngine.enableLeader();
    }

    @AfterEach
    void tearDown() throws IOException {
        if (asyncClientSocket != null) {
            asyncClientSocket.close();
        }
        if (rulesEngine != null) {
            rulesEngine.dispose(sessionId);
            rulesEngine.close();
        }
        cleanupDatabase();
    }

    @Test
    void testLargeEventInsertionMatch() {
        for (int i = 0; i < EVENT_COUNT; i++) {
            String event = createLargeTriggerEvent(i, BLOB_SIZE, true);
            String result = rulesEngine.assertEvent(sessionId, event);

            List<Map<String, Object>> matchList = readValueAsListOfMapOfStringAndObject(result);
            assertThat(matchList).hasSize(1);
            assertThat(matchList.get(0)).containsEntry("name", "always_fire");

            String matchingUuid = extractMatchingUuidFromResponse(result);
            assertThat(matchingUuid).isNotBlank();
        }

        // Allow some memory for the processing overhead (This is brittle threshold, can be adjusted when fail)
        long acceptableMemoryOverhead = 65 * 1000 * 1024; // 65 MB
        System.gc();
        Map<String, Object> statsMap = JsonMapper.readValueAsMapOfStringAndObject(rulesEngine.sessionStats(sessionId));
        long baseLevelMemory = (Integer)statsMap.get("baseLevelMemory");
        long usedMemory = (Integer)statsMap.get("usedMemory");
        System.out.println("baseLevelMemory = " + baseLevelMemory);
        System.out.println("usedMemory      = " + usedMemory);
        assertThat(usedMemory).isLessThan(baseLevelMemory + acceptableMemoryOverhead);

        String eventRecordRowCount = TestUtils.queryRawColumn(dbParams,
                                                              "SELECT COUNT(*) FROM drools_ansible_event_record WHERE ha_uuid = ?", HA_UUID);
        assertThat(eventRecordRowCount).isEqualTo("0");
    }

    @Test
    void testLargeEventInsertionUnmatch() {
        for (int i = 0; i < EVENT_COUNT; i++) {
            String event = createLargeTriggerEvent(i, BLOB_SIZE, false);
            String result = rulesEngine.assertEvent(sessionId, event);

            List<Map<String, Object>> matchList = readValueAsListOfMapOfStringAndObject(result);
            assertThat(matchList).isEmpty();
        }

        List<String> partialEventIds = JsonMapper.readValue(rulesEngine.getPartialEventIds(sessionId), List.class);
        assertThat(partialEventIds).isEmpty();

        // Allow some memory for the processing overhead
        long acceptableMemoryOverhead = 65 * 1000 * 1024; // 65 MB (This is brittle threshold, can be adjusted when fail)
        System.gc();
        Map<String, Object> statsMap = JsonMapper.readValueAsMapOfStringAndObject(rulesEngine.sessionStats(sessionId));
        long baseLevelMemory = (Integer)statsMap.get("baseLevelMemory");
        long usedMemory = (Integer)statsMap.get("usedMemory");
        System.out.println("baseLevelMemory = " + baseLevelMemory);
        System.out.println("usedMemory      = " + usedMemory);
        assertThat(usedMemory).isLessThan(baseLevelMemory + acceptableMemoryOverhead);

        String eventRecordRowCount = TestUtils.queryRawColumn(dbParams,
                "SELECT COUNT(*) FROM drools_ansible_event_record WHERE ha_uuid = ?", HA_UUID);
        assertThat(eventRecordRowCount).isEqualTo("0");
    }

    private static String createLargeTriggerEvent(int sequence, int blobSize, boolean trigger) {
        String payload = "x".repeat(blobSize);
        return createEvent("""
                {
                    "trigger": %b,
                    "sequence": %d,
                    "blob": "%s"
                }
                """.formatted(trigger, sequence, payload));
    }
}
