package org.drools.ansible.rulebook.integration.ha.tests.integration.perf;

import java.io.IOException;
import java.net.Socket;

import org.drools.ansible.rulebook.integration.api.RuleConfigurationOption;
import org.drools.ansible.rulebook.integration.core.jpy.AstRulesEngine;
import org.drools.ansible.rulebook.integration.ha.tests.support.AbstractHATestBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.drools.ansible.rulebook.integration.ha.tests.support.TestUtils.createEvent;

class HAIntegrationLargePartialMatchTest extends AbstractHATestBase {

    private static final String HA_UUID = "large-partial-match-ha";
    private static final int LARGE_PARTIAL_EVENT_COUNT = 1000;
    private static final int PARTIAL_EVENT_BLOB_SIZE = 24 * 1024;

    private static final String RULE_SET_LARGE_PARTIAL_EVENTS = """
            {
                "name": "Large Partial Event Ruleset",
                "rules": [
                    {"Rule": {
                        "name": "large_partial_event_rule",
                        "condition": {
                            "AllCondition": [
                                {
                                    "EqualsExpression": {
                                        "lhs": {
                                            "Event": "phase"
                                        },
                                        "rhs": {
                                            "String": "partial"
                                        }
                                    }
                                },
                                {
                                    "EqualsExpression": {
                                        "lhs": {
                                            "Event": "complete"
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
                                    "name": "noop.yml"
                                }
                            ]
                        }
                    }}
                ]
            }
            """;

    static {
        System.setProperty("org.slf4j.simpleLogger.log.org.drools.ansible.rulebook.integration", "WARN");

        if (USE_POSTGRES) {
            initializePostgres("eda_ha_test", "HA large partial match tests");
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

        // HA leader startup requires an async client connection even though this test
        // exercises the synchronous assertEvent path.
        asyncClientSocket = new Socket("localhost", rulesEngine.port());

        rulesEngine.initializeHA(HA_UUID, "worker-1", dbParamsJson, dbHAConfigJson);
        sessionId = rulesEngine.createRuleset(RULE_SET_LARGE_PARTIAL_EVENTS,
                RuleConfigurationOption.FULLY_MANUAL_PSEUDOCLOCK);
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

    @Disabled("Just to check the response time")
    @Test
    void testLargePartialEventLastResponseTime() {
        long lastResponseNanos = 0L;
        String lastResponse = null;

        for (int i = 0; i < LARGE_PARTIAL_EVENT_COUNT; i++) {
            String event = createLargePartialEvent(i, PARTIAL_EVENT_BLOB_SIZE);
            long start = System.nanoTime();
            lastResponse = rulesEngine.assertEvent(sessionId, event);
            lastResponseNanos = System.nanoTime() - start;
        }

        double lastResponseMillis = lastResponseNanos / 1_000_000.0;
        System.out.printf("Large partial event test: count=%d payloadBytes=%d lastResponseMs=%.3f%n",
                LARGE_PARTIAL_EVENT_COUNT, PARTIAL_EVENT_BLOB_SIZE, lastResponseMillis);
        System.out.println("Large partial event test last response: " + lastResponse);
    }

    private static String createLargePartialEvent(int sequence, int blobSize) {
        String payload = "x".repeat(blobSize);
        return createEvent("""
                {
                    "phase": "partial",
                    "sequence": %d,
                    "blob": "%s"
                }
                """.formatted(sequence, payload));
    }
}
