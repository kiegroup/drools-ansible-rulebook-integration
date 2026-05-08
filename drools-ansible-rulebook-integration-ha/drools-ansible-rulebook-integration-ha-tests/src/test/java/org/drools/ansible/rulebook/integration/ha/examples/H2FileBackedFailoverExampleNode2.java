package org.drools.ansible.rulebook.integration.ha.examples;

import org.drools.ansible.rulebook.integration.core.jpy.AstRulesEngine;
import org.drools.ansible.rulebook.integration.ha.tests.HAIntegrationTestBase;
import org.drools.ansible.rulebook.integration.ha.tests.TestUtils;

/**
 * Node-2 recovers from the H2 file after Node-1 crashes.
 * Run H2FileBackedFailoverExampleNode1 first, Ctrl+C it, then run this.
 *
 * To run:
 *   mvn exec:java -pl drools-ansible-rulebook-integration-ha/drools-ansible-rulebook-integration-ha-tests \
 *     -Dexec.mainClass="org.drools.ansible.rulebook.integration.ha.examples.H2FileBackedFailoverExampleNode2" \
 *     -Dexec.classpathScope=test
 */
public class H2FileBackedFailoverExampleNode2 {

    public static void main(String[] args) throws Exception {
        Thread.currentThread().setName("Node-2");

        System.out.println("╔════════════════════════════════════════════════╗");
        System.out.println("║   NODE-2 (H2 File-Backed) RECOVERING          ║");
        System.out.println("╚════════════════════════════════════════════════╝");
        System.out.println("[Node-2] H2 file: " + H2FileBackedFailoverExampleNode1.DB_FILE_PATH);

        String dbParamsJson = """
                {
                    "db_type": "h2",
                    "db_file_path": "%s"
                }
                """.formatted(H2FileBackedFailoverExampleNode1.DB_FILE_PATH);

        String configJson = """
                {
                    "write_after": 1
                }
                """;

        AstRulesEngine rulesEngine = new AstRulesEngine();
        HAIntegrationTestBase.AsyncConsumer consumer = null;

        try {
            System.out.println("\n[Node-2] Initializing HA (same UUID as Node-1)...");
            rulesEngine.initializeHA(H2FileBackedFailoverExampleNode1.HA_UUID, "worker-2", dbParamsJson, configJson);
            System.out.println("[Node-2] HA initialized");

            System.out.println("[Node-2] Creating ruleset (same rules as Node-1)...");
            long sessionId = rulesEngine.createRuleset(H2FileBackedFailoverExampleNode1.RULE_SET);
            System.out.println("[Node-2] Ruleset created with session ID: " + sessionId);

            System.out.println("[Node-2] Starting async consumer...");
            consumer = new HAIntegrationTestBase.AsyncConsumer("node-2-consumer");
            consumer.startConsuming(rulesEngine.port());
            System.out.println("[Node-2] Async consumer started on port: " + rulesEngine.port());

            System.out.println("\n[Node-2] Becoming LEADER (recovering from Node-1)...");
            rulesEngine.enableLeader();
            System.out.println("[Node-2] I am now the LEADER");

            // Wait for async recovery dispatch
            Thread.sleep(2000);

            System.out.println("\n[Node-2] Recovered async messages: " + consumer.getReceivedMessages().size());
            for (int i = 0; i < consumer.getReceivedMessages().size(); i++) {
                System.out.println("[Node-2]   Message " + i + ": " + consumer.getReceivedMessages().get(i));
            }

            String statsJson = rulesEngine.getHAStats();
            System.out.println("\n[Node-2] HA Stats: " + statsJson);

            // Step: Complete the partial match from Node-1 by sending the missing humidity event
            System.out.println("\n[Node-2] Sending humidity event to complete partial match from Node-1...");

            String event = """
                    {"humidity": 60, "location": "datacenter-2"}
                    """;
            String response = rulesEngine.assertEvent(sessionId, TestUtils.createEvent(event));
            System.out.println("[Node-2]   Response: " + response);

            String uuid = TestUtils.extractMatchingUuidFromResponse(response);
            if (uuid != null && !uuid.isEmpty()) {
                System.out.println("[Node-2]   MATCH! Partial match from Node-1 completed. matching_uuid: " + uuid);
            } else {
                System.out.println("[Node-2]   No match (partial events may not have been recovered)");
            }

            statsJson = rulesEngine.getHAStats();
            System.out.println("\n[Node-2] Final HA Stats: " + statsJson);

            System.out.println("\n[Node-2] Failover test complete. Shutting down...");
        } finally {
            if (consumer != null) {
                consumer.stop();
            }
            rulesEngine.disableLeader();
            rulesEngine.shutdown();
            System.out.println("[Node-2] Done.");
            System.out.println("\nTo clean up: rm -f ./data/eda_ha.mv.db ./data/eda_ha.trace.db");
        }
    }
}
