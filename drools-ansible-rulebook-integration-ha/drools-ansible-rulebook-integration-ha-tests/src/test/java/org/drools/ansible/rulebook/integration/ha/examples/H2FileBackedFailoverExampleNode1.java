package org.drools.ansible.rulebook.integration.ha.examples;

import org.drools.ansible.rulebook.integration.core.jpy.AstRulesEngine;
import org.drools.ansible.rulebook.integration.ha.tests.HAIntegrationTestBase;
import org.drools.ansible.rulebook.integration.ha.tests.TestUtils;

/**
 * Node-1 in a file-backed H2 failover test.
 * Run this first, then Ctrl+C to simulate a crash, then run H2FileBackedFailoverExampleNode2.
 *
 * To run:
 *   mvn exec:java -pl drools-ansible-rulebook-integration-ha/drools-ansible-rulebook-integration-ha-tests \
 *     -Dexec.mainClass="org.drools.ansible.rulebook.integration.ha.examples.H2FileBackedFailoverExampleNode1" \
 *     -Dexec.classpathScope=test
 *
 * To clean up:
 *   rm -f ./data/eda_ha.mv.db ./data/eda_ha.trace.db
 */
public class H2FileBackedFailoverExampleNode1 {

    public static final String HA_UUID = "h2-file-cluster";
    public static final String DB_FILE_PATH = "./data/eda_ha";

    public static final String RULE_SET = """
            {
              "name": "temperature_monitoring",
              "rules": [
                {"Rule": {
                  "name": "high_temperature_alert",
                  "condition": {
                    "AllCondition": [
                      {
                        "GreaterThanExpression": {
                          "lhs": {
                            "Event": "temperature"
                          },
                          "rhs": {
                            "Integer": 30
                          }
                        }
                      },
                      {
                        "GreaterThanExpression": {
                          "lhs": {
                            "Event": "humidity"
                          },
                          "rhs": {
                            "Integer": 50
                          }
                        }
                      }
                    ]
                  },
                  "action": {
                    "run_playbook": [
                      {
                        "name": "send_alert.yml"
                      }
                    ]
                  }
                }}
              ]
            }
            """;

    public static void main(String[] args) throws Exception {
        Thread.currentThread().setName("Node-1");

        System.out.println("╔════════════════════════════════════════════════╗");
        System.out.println("║   NODE-1 (H2 File-Backed) STARTING            ║");
        System.out.println("╚════════════════════════════════════════════════╝");
        System.out.println("[Node-1] H2 file: " + DB_FILE_PATH);

        String dbParamsJson = """
                {
                    "db_type": "h2",
                    "db_file_path": "%s"
                }
                """.formatted(DB_FILE_PATH);

        String configJson = """
                {
                    "write_after": 1
                }
                """;

        AstRulesEngine rulesEngine = new AstRulesEngine();
        HAIntegrationTestBase.AsyncConsumer consumer = null;

        try {
            System.out.println("[Node-1] Initializing HA...");
            rulesEngine.initializeHA(HA_UUID, "worker-1", dbParamsJson, configJson);
            System.out.println("[Node-1] HA initialized with UUID: " + HA_UUID);

            System.out.println("[Node-1] Creating ruleset...");
            long sessionId = rulesEngine.createRuleset(RULE_SET);
            System.out.println("[Node-1] Ruleset created with session ID: " + sessionId);

            System.out.println("[Node-1] Starting async consumer...");
            consumer = new HAIntegrationTestBase.AsyncConsumer("node-1-consumer");
            consumer.startConsuming(rulesEngine.port());
            System.out.println("[Node-1] Async consumer started on port: " + rulesEngine.port());

            System.out.println("\n[Node-1] Becoming LEADER...");
            rulesEngine.enableLeader();
            System.out.println("[Node-1] I am now the LEADER\n");

            // Step 1: Send event that creates a full match (temperature > 30 AND humidity > 50)
            System.out.println("[Node-1] Step 1: Sending 2 events for a full match...");

            String event1 = """
                    {"temperature": 35, "location": "datacenter-1"}
                    """;
            String response1 = rulesEngine.assertEvent(sessionId, TestUtils.createEvent(event1));
            System.out.println("[Node-1]   Event 1 (temperature=35): " + response1);

            String event2 = """
                    {"humidity": 55, "location": "datacenter-1"}
                    """;
            String response2 = rulesEngine.assertEvent(sessionId, TestUtils.createEvent(event2));
            System.out.println("[Node-1]   Event 2 (humidity=55): " + response2);

            String uuid = TestUtils.extractMatchingUuidFromResponse(response2);
            if (uuid != null && !uuid.isEmpty()) {
                System.out.println("[Node-1]   MATCH! matching_uuid: " + uuid);
                String action = """
                        {"action": "send_alert", "status": 1, "message": "started"}
                        """;
                rulesEngine.addActionInfo(sessionId, uuid, 0, action);
                System.out.println("[Node-1]   Action added (status=1: started) -- pending in DB");
            }

            // Step 2: Send event that creates a partial match (only temperature, no humidity yet)
            System.out.println("\n[Node-1] Step 2: Sending 1 event for a partial match...");

            String event3 = """
                    {"temperature": 40, "location": "datacenter-2"}
                    """;
            String response3 = rulesEngine.assertEvent(sessionId, TestUtils.createEvent(event3));
            System.out.println("[Node-1]   Event 3 (temperature=40): " + response3);
            System.out.println("[Node-1]   Partial match stored in session state");

            String statsJson = rulesEngine.getHAStats();
            System.out.println("\n[Node-1] HA Stats: " + statsJson);

            // Wait for crash
            System.out.println("\n[Node-1] State is persisted to H2 file: " + DB_FILE_PATH + ".mv.db");
            System.out.println("[Node-1] Press Ctrl+C to simulate crash, then run Node2 to recover.");

            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
            System.out.println("\n[Node-1] Interrupted! Simulating crash (no graceful shutdown).");
        } finally {
            if (consumer != null) {
                consumer.stop();
            }
            // Intentionally NOT calling disableLeader() or shutdown() to simulate crash
            // But we do close the datasource to release the H2 file lock
            rulesEngine.shutdown();
            System.out.println("[Node-1] Done.");
        }
    }
}
