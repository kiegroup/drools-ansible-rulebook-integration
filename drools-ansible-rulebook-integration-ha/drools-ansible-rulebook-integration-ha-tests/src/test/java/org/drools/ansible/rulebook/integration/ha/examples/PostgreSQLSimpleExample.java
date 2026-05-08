package org.drools.ansible.rulebook.integration.ha.examples;

import org.drools.ansible.rulebook.integration.api.RuleConfigurationOption;
import org.drools.ansible.rulebook.integration.core.jpy.AstRulesEngine;
import org.drools.ansible.rulebook.integration.ha.tests.HAIntegrationTestBase;
import org.drools.ansible.rulebook.integration.ha.tests.TestUtils;

/**
 * Simple example demonstrating EDA HA with PostgreSQL.
 *
 * Prerequisites:
 * 1. Start PostgreSQL using docker-compose:
 *    docker-compose up -d
 *
 * 2. Run this example:
 *    mvn compile exec:java -Dexec.mainClass="org.drools.ansible.rulebook.integration.ha.examples.PostgreSQLSimpleExample"
 *
 * This example:
 * - Initializes HA mode with PostgreSQL
 * - Creates a ruleset with a simple temperature rule
 * - Enables leader mode
 * - Sends an event that triggers the rule
 * - Shows the matching event with UUID
 */
public class PostgreSQLSimpleExample {

    private static final String HA_UUID = "example-ha-cluster-001";

    private static final String RULE_SET = """
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
        // Set a simple thread name for cleaner log output
        Thread.currentThread().setName("main");

        System.out.println("=== EDA HA PostgreSQL Simple Example ===\n");

        // Step 1: Create AstRulesEngine instance
        System.out.println("Step 1: Creating AstRulesEngine instance...");
        AstRulesEngine rulesEngine = new AstRulesEngine();
        HAIntegrationTestBase.AsyncConsumer consumer = null;

        // Step 2: Initialize HA mode with PostgreSQL
        System.out.println("Step 2: Initializing HA mode with PostgreSQL...");
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

        rulesEngine.initializeHA(HA_UUID, "worker-1", dbParamsJson, configJson);
        System.out.println("   HA initialized with UUID: " + HA_UUID);

        // Step 3: Create ruleset
        System.out.println("\nStep 3: Creating ruleset...");
        long sessionId = rulesEngine.createRuleset(RULE_SET);
        System.out.println("   Ruleset created with session ID: " + sessionId);

        // Step 3b: Start async consumer to handle rule matches
        System.out.println("\nStep 3b: Starting async consumer...");
        consumer = new HAIntegrationTestBase.AsyncConsumer("example-consumer");
        consumer.startConsuming(rulesEngine.port());
        System.out.println("   Async consumer started on port: " + rulesEngine.port());

        // Step 4: Enable leader mode
        System.out.println("\nStep 4: Enabling leader mode...");
        rulesEngine.enableLeader();
        System.out.println("   Leader mode enabled for: worker-1");

        // Step 5: Get initial HA stats
        System.out.println("\nStep 5: Checking initial HA stats...");
        String statsJson = rulesEngine.getHAStats();
        System.out.println("   HA Stats: " + statsJson);

        // Step 6: Send an event
        System.out.println("\nStep 6: Sending temperature event...");
        String event = """
                {
                    "temperature": 35,
                    "location": "datacenter-1"
                }
                """;

        String response = rulesEngine.assertEvent(sessionId, TestUtils.createEvent(event));
        System.out.println("   Event processed successfully!");
        System.out.println("   Response: " + response);

        // Step 7: Check HA stats after event
        System.out.println("\nStep 7: Checking HA stats after event...");
        statsJson = rulesEngine.getHAStats();
        System.out.println("   HA Stats: " + statsJson);

        // Step 7b: Extract matching_uuid from response for action handling
        String matchingUuid = TestUtils.extractMatchingUuidFromResponse(response);
        System.out.println("\nStep 7b: Extracted matching_uuid: " + matchingUuid);

        // Step 7c: Action handling - Add action
        System.out.println("\nStep 7c: Adding action info...");
        String action1 = """
                {
                    "action": "send_alert",
                    "status": 1,
                    "message": "Alert email sent"
                }
                """;
        rulesEngine.addActionInfo(sessionId, matchingUuid, 0, action1);
        System.out.println("   Action added at index 0");

        // Step 7d: Check if action exists
        System.out.println("\nStep 7d: Checking if action exists...");
        boolean exists = rulesEngine.actionInfoExists(sessionId, matchingUuid, 0);
        System.out.println("   Action exists: " + exists);

        // Step 7e: Get action info
        System.out.println("\nStep 7e: Getting action info...");
        String retrievedAction = rulesEngine.getActionInfo(sessionId, matchingUuid, 0);
        System.out.println("   Retrieved action: " + retrievedAction);

        // Step 7f: Update action status
        System.out.println("\nStep 7f: Updating action status to success...");
        String updatedAction = """
                {
                    "action": "send_alert",
                    "status": 3,
                    "message": "Alert email sent successfully"
                }
                """;
        rulesEngine.updateActionInfo(sessionId, matchingUuid, 0, updatedAction);
        System.out.println("   Action updated");

        // Step 7g: Get updated action status
        System.out.println("\nStep 7g: Getting action status...");
        String status = rulesEngine.getActionStatus(sessionId, matchingUuid, 0);
        System.out.println("   Action status: " + status);

        // Step 7h: Check HA stats after action
        System.out.println("\nStep 7h: Checking HA stats after action processing...");
        statsJson = rulesEngine.getHAStats();
        System.out.println("   HA Stats: " + statsJson);

        // Step 7i: Delete action (signals action completion)
        System.out.println("\nStep 7i: Deleting action info (action complete)...");
        rulesEngine.deleteActionInfo(sessionId, matchingUuid);
        System.out.println("   Action deleted (MatchingEvent and ActionInfo removed)");

        // Step 8: Send another event that doesn't match
        System.out.println("\nStep 8: Sending event that doesn't match...");
        String event2 = """
                {
                    "temperature": 25,
                    "location": "datacenter-2"
                }
                """;

        String response2 = rulesEngine.assertEvent(sessionId, TestUtils.createEvent(event2));
        System.out.println("   Event processed successfully!");
        System.out.println("   Response: " + response2);

        // Step 9: Wait a bit to see async responses
        System.out.println("\nStep 9: Waiting for async responses...");
        Thread.sleep(1000);
        System.out.println("   Async messages received: " + consumer.getReceivedMessages().size());
        if (!consumer.getReceivedMessages().isEmpty()) {
            System.out.println("   First async response: " + consumer.getReceivedMessages().get(0));
        }

        // Step 10: Disable leader and shutdown
        System.out.println("\nStep 10: Shutting down...");
        if (consumer != null) {
            consumer.stop();
        }
        rulesEngine.disableLeader();
        rulesEngine.shutdown();
        System.out.println("   Shutdown complete.");

        System.out.println("\n=== Example completed successfully! ===");
        System.out.println("\nYou can now connect to PostgreSQL and check the data:");
        System.out.println("  docker exec -it eda-ha-postgres psql -U eda_user -d eda_ha_db");
        System.out.println("  SELECT * FROM SessionState;");
        System.out.println("  SELECT * FROM MatchingEvent;");
        System.out.println("  SELECT * FROM HAStats;");
    }
}
