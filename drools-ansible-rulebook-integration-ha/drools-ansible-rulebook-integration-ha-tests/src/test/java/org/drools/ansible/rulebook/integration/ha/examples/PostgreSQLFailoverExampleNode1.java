package org.drools.ansible.rulebook.integration.ha.examples;

import org.drools.ansible.rulebook.integration.api.RuleConfigurationOption;
import org.drools.ansible.rulebook.integration.core.jpy.AstRulesEngine;
import org.drools.ansible.rulebook.integration.ha.tests.HAIntegrationTestBase;
import org.drools.ansible.rulebook.integration.ha.tests.TestUtils;

/**
 * Node-1 in a two-node HA cluster.
 * Run this first, then run PostgreSQLFailoverExampleNode2 in another terminal.
 *
 * To run:
 *   mvn exec:java -pl drools-ansible-rulebook-integration-ha/drools-ansible-rulebook-integration-ha-tests \
 *     -Dexec.mainClass="org.drools.ansible.rulebook.integration.ha.examples.PostgreSQLFailoverExampleNode1" \
 *     -Dexec.classpathScope=test
 */
public class PostgreSQLFailoverExampleNode1 {

    public static final String HA_UUID = "two-node-cluster";

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
                        "name": "send_alert.yml",
                        "extra_vars": {
                          "message": "High temperature detected"
                        }
                      }
                    ]
                  }
                }}
              ]
            }
            """;

    public static void main(String[] args) throws Exception {
        Thread.currentThread().setName("Node-1");

        System.out.println("╔════════════════════════════════════════╗");
        System.out.println("║           NODE-1 STARTING              ║");
        System.out.println("╚════════════════════════════════════════╝\n");

        String dbParamsJson = """
                {
                    "db_type": "postgres",
                    "host": "localhost",
                    "port": 5432,
                    "database": "eda_ha_db",
                    "user": "eda_user",
                    "password": "eda_password",
                    "sslmode": "disable"
                }
                """;

        String configJson = """
                {
                    "write_after": 1
                }
                """;

        AstRulesEngine rulesEngine = new AstRulesEngine();
        HAIntegrationTestBase.AsyncConsumer consumer = null;

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
        System.out.println("[Node-1] ✓ I am now the LEADER\n");

        String statsJson = rulesEngine.getHAStats();
        System.out.println("[Node-1] HA Stats: " + statsJson);

        // Scenario Step 1: Send 2 events that match the rule (temperature > 30 AND humidity > 50)
        System.out.println("\n[Node-1] Step 1: Sending 2 events that create a match...");

        String event1 = """
                {
                    "temperature": 35,
                    "location": "datacenter-1"
                }
                """;
        String response1 = rulesEngine.assertEvent(sessionId, TestUtils.createEvent(event1));
        System.out.println("[Node-1] ✓ Event 1 processed (temperature=35)");
        System.out.println("[Node-1]   No Match yet: " + response1);

        String event2 = """
                {
                    "humidity": 55,
                    "location": "datacenter-1"
                }
                """;
        String response2 = rulesEngine.assertEvent(sessionId, TestUtils.createEvent(event2));
        System.out.println("[Node-1] ✓ Event 2 processed (humidity=55)");
        System.out.println("[Node-1]   Response: " + response2);

        String uuid2 = TestUtils.extractMatchingUuidFromResponse(response2);
        if (uuid2 != null && !uuid2.isEmpty()) {
            System.out.println("[Node-1] ✓ MATCH! matching_uuid: " + uuid2);

            String action1 = """
                    {
                        "action": "send_alert",
                        "status": 1,
                        "message": "Alert started for datacenter-1"
                    }
                    """;
            rulesEngine.addActionInfo(sessionId, uuid2, 0, action1);
            System.out.println("[Node-1] ✓ Action added (status=1: started)");
        } else {
            System.out.println("[Node-1] ✓ No match (partial match stored in session state)");
        }

        Thread.sleep(2000);

        // Scenario Step 2: Send 1 event that creates a partial match (only temperature, no humidity)
        System.out.println("\n[Node-1] Step 2: Sending 1 event that creates partial match...");

        String event3 = """
                {
                    "temperature": 40,
                    "location": "datacenter-2"
                }
                """;
        String response3 = rulesEngine.assertEvent(sessionId, TestUtils.createEvent(event3));
        System.out.println("[Node-1] ✓ Event 3 processed (temperature=40)");
        System.out.println("[Node-1]   Response: " + response3);

        String uuid3 = TestUtils.extractMatchingUuidFromResponse(response3);
        if (uuid3 != null && !uuid3.isEmpty()) {
            System.out.println("[Node-1] ✓ MATCH! matching_uuid: " + uuid3);
        } else {
            System.out.println("[Node-1] ✓ No match (partial match stored in session state)");
            System.out.println("[Node-1]   Waiting for humidity event to complete the match...");
        }

        statsJson = rulesEngine.getHAStats();
        System.out.println("\n[Node-1] Current HA Stats: " + statsJson);

        // Scenario Step 3: Node-1 crashes
        System.out.println("\n[Node-1] Step 3: Ready to simulate failure...");
        System.out.println("[Node-1] Start Node-2 in another terminal, then press Ctrl+C here to crash.");
        System.out.println("[Node-1] Node-2 should recover the pending action and partial match.\n");

        // Keep running until interrupted
        try {
            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
            System.out.println("\n[Node-1] Interrupted! Shutting down...");
        } finally {
            if (consumer != null) {
                consumer.stop();
            }
            rulesEngine.disableLeader();
            rulesEngine.shutdown();
            System.out.println("[Node-1] Shut down gracefully");
        }
    }
}
