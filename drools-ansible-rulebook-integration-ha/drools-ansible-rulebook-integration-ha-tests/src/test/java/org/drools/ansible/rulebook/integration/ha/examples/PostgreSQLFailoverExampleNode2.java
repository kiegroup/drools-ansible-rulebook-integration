package org.drools.ansible.rulebook.integration.ha.examples;

import org.drools.ansible.rulebook.integration.api.RuleConfigurationOption;
import org.drools.ansible.rulebook.integration.core.jpy.AstRulesEngine;
import org.drools.ansible.rulebook.integration.ha.tests.integration.HAIntegrationTestBase;
import org.drools.ansible.rulebook.integration.ha.tests.support.TestUtils;

/**
 * Node-2 in a two-node HA cluster (standby/follower).
 * Run PostgreSQLFailoverExampleNode1 first, then run this in another terminal.
 * When Node-1 is stopped (Ctrl+C), this node will detect and take over as leader.
 *
 * To run:
 *   mvn exec:java -pl drools-ansible-rulebook-integration-ha/drools-ansible-rulebook-integration-ha-tests \
 *     -Dexec.mainClass="org.drools.ansible.rulebook.integration.ha.examples.PostgreSQLFailoverExampleNode2" \
 *     -Dexec.classpathScope=test
 */
public class PostgreSQLFailoverExampleNode2 {

    public static void main(String[] args) throws Exception {
        Thread.currentThread().setName("Node-2");

        System.out.println("╔════════════════════════════════════════╗");
        System.out.println("║           NODE-2 STARTING              ║");
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

        System.out.println("[Node-2] Initializing HA...");
        rulesEngine.initializeHA(PostgreSQLFailoverExampleNode1.HA_UUID, "worker-2", dbParamsJson, configJson);
        System.out.println("[Node-2] HA initialized with UUID: " + PostgreSQLFailoverExampleNode1.HA_UUID);

        System.out.println("[Node-2] Creating ruleset...");
        long sessionId = rulesEngine.createRuleset(PostgreSQLFailoverExampleNode1.RULE_SET);
        System.out.println("[Node-2] Ruleset created with session ID: " + sessionId);

        System.out.println("[Node-2] Starting async consumer...");
        consumer = new HAIntegrationTestBase.AsyncConsumer("node-2-consumer");
        consumer.startConsuming(rulesEngine.port());
        System.out.println("[Node-2] Async consumer started on port: " + rulesEngine.port());

        System.out.println("\n[Node-2] Running as STANDBY (not a leader yet)");
        System.out.println("[Node-2] Waiting for leader election signal...\n");

        // Simulate waiting for leader election trigger
        // In production, this would be triggered by external orchestration (e.g., Kubernetes, consul)
        System.out.println("[Node-2] Press Enter to become leader (simulating Node-1 failure detection)...");
        System.in.read();

        // Scenario Step 4: Node-2 becomes leader
        System.out.println("\n[Node-2] Step 4: Node-1 failure detected! Taking over as LEADER...");
        rulesEngine.enableLeader();
        System.out.println("[Node-2] ✓ I am now the LEADER");
        System.out.println("[Node-2] ✓ SessionState recovered from PostgreSQL (including partial match)");
        System.out.println("[Node-2] ✓ Pending MatchingEvents dispatched via async channel\n");

        // Scenario Step 5: Receive pending match information
        Thread.sleep(2000);

        System.out.println("[Node-2] Step 5: Checking async recovery messages...");
        System.out.println("[Node-2] Async messages received: " + consumer.getReceivedMessages().size());

        String recoveredMatchingUuid = null;
        for (int i = 0; i < consumer.getReceivedMessages().size(); i++) {
            String msg = consumer.getReceivedMessages().get(i);
            System.out.println("[Node-2]   Message " + (i + 1) + ": " + msg);

            // Extract matching_uuid from the first recovery message
            if (i == 0 && recoveredMatchingUuid == null) {
                recoveredMatchingUuid = TestUtils.extractMatchingUuidFromAsyncRecoveryMessage(msg);
                if (recoveredMatchingUuid != null && !recoveredMatchingUuid.isEmpty()) {
                    System.out.println("[Node-2] ✓ Extracted matching_uuid from recovery: " + recoveredMatchingUuid);
                }
            }
        }

        String statsJson = rulesEngine.getHAStats();
        System.out.println("\n[Node-2] HA Stats after takeover: " + statsJson);

        // Scenario Step 6: Get/Update/Delete the action (continuing action management after fail-over)
        if (recoveredMatchingUuid != null && !recoveredMatchingUuid.isEmpty()) {
            System.out.println("\n[Node-2] Step 6: Managing recovered action...");

            // Get the existing action
            String existingAction = rulesEngine.getActionInfo(sessionId, recoveredMatchingUuid, 0);
            System.out.println("[Node-2] ✓ Retrieved existing action: " + existingAction);

            // Update the action status
            String updatedAction = """
                    {
                        "action": "send_alert",
                        "status": 3,
                        "message": "Alert completed by Node-2 after takeover"
                    }
                    """;
            rulesEngine.updateActionInfo(sessionId, recoveredMatchingUuid, 0, updatedAction);
            System.out.println("[Node-2] ✓ Updated action (status=3: success)");

            // Delete the action (mark as complete)
            rulesEngine.deleteActionInfo(sessionId, recoveredMatchingUuid);
            System.out.println("[Node-2] ✓ Action completed and deleted");
        } else {
            System.out.println("[Node-2] ✗ No recovered matching_uuid found, skipping action management");
        }

        // Scenario Step 7: Send humidity event to complete the partial match
        System.out.println("\n[Node-2] Step 7: Sending humidity event to complete partial match...");

        String event4 = """
                {
                    "humidity": 60,
                    "location": "datacenter-2"
                }
                """;
        String response4 = rulesEngine.assertEvent(sessionId, TestUtils.createEvent(event4));
        System.out.println("[Node-2] ✓ Event 4 processed (humidity=60)");
        System.out.println("[Node-2]   Response: " + response4);

        String uuid4 = TestUtils.extractMatchingUuidFromResponse(response4);
        if (uuid4 != null && !uuid4.isEmpty()) {
            System.out.println("[Node-2] ✓ MATCH! Partial match completed!");
            System.out.println("[Node-2]   matching_uuid: " + uuid4);
            System.out.println("[Node-2]   (temperature=40 from Node-1 + humidity=60 from Node-2)");
        } else {
            System.out.println("[Node-2] ✗ No match (unexpected!)");
        }

        statsJson = rulesEngine.getHAStats();
        System.out.println("\n[Node-2] Final HA Stats: " + statsJson);

        System.out.println("\n[Node-2] Running as leader... (Press Ctrl+C to stop)\n");

        // Keep running until interrupted
        try {
            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
            System.out.println("\n[Node-2] Interrupted! Shutting down...");
        } finally {
            if (consumer != null) {
                consumer.stop();
            }
            rulesEngine.disableLeader();
            rulesEngine.shutdown();
            System.out.println("[Node-2] Shut down gracefully");
        }
    }
}
