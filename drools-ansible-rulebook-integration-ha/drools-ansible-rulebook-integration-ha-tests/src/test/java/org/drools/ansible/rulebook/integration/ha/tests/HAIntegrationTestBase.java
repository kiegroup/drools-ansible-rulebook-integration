package org.drools.ansible.rulebook.integration.ha.tests;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.drools.ansible.rulebook.integration.api.RuleConfigurationOption;
import org.drools.ansible.rulebook.integration.core.jpy.AstRulesEngine;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManager;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManagerFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.readValueAsMapOfStringAndObject;

/**
 * Base class for AstRulesEngine HA integration tests.
 * Supports both H2 and PostgreSQL based on system property 'test.db.type'.
 *
 * Usage:
 * - Default (H2): mvn test
 * - PostgreSQL: mvn test -Dtest.db.type=postgres
 */
public abstract class HAIntegrationTestBase extends AbstractHATestBase {

    protected static final String HA_UUID = "integration-ha-1";

    // Static initialization - runs once for all test classes
    static {
        if (USE_POSTGRES) {
            initializePostgres("eda_ha_test", "HA integration tests");
        } else {
            initializeH2();
        }
    }

    protected AstRulesEngine rulesEngine1; // node 1
    protected AstRulesEngine rulesEngine2; // node 2

    protected long sessionId1; // node1
    protected long sessionId2; // node2

    protected AsyncConsumer consumer1; // node1
    protected AsyncConsumer consumer2; // node2

    abstract String getRuleSet();

    @BeforeEach
    void setUp() {
        System.out.println("Running test with database: " + TEST_DB_TYPE);

        rulesEngine1 = new AstRulesEngine();

        consumer1 = new AsyncConsumer("consumer1");
        consumer1.startConsuming(rulesEngine1.port());

        rulesEngine1.initializeHA(HA_UUID, "worker-1", dbParamsJson, dbHAConfigJson); // The same cluster. Both nodes share same DB
        sessionId1 = rulesEngine1.createRuleset(getRuleSet(), RuleConfigurationOption.FULLY_MANUAL_PSEUDOCLOCK);

        rulesEngine2 = new AstRulesEngine();

        consumer2 = new AsyncConsumer("consumer2");
        consumer2.startConsuming(rulesEngine2.port());

        rulesEngine2.initializeHA(HA_UUID, "worker-2", dbParamsJson, dbHAConfigJson); // The same cluster. Both nodes share same DB
        sessionId2 = rulesEngine2.createRuleset(getRuleSet(), RuleConfigurationOption.FULLY_MANUAL_PSEUDOCLOCK);
    }

    @AfterEach
    void tearDown() {
        if (consumer1 != null) {
            consumer1.stop();
        }
        if (consumer2 != null) {
            consumer2.stop();
        }

        if (rulesEngine1 != null) {
            rulesEngine1.dispose(sessionId1);
            rulesEngine1.close(); // Close connection pools
        }
        if (rulesEngine2 != null) {
            rulesEngine2.dispose(sessionId2);
            rulesEngine2.close(); // Close connection pools
        }

        // Clean up database using inherited method
        cleanupDatabase();
    }

    // Simulate a python client that consumes async responses
    public static class AsyncConsumer {
        private volatile boolean keepReading = true;
        private final ExecutorService executor = Executors.newSingleThreadExecutor();
        private final String name;
        private final List<String> receivedMessages = new ArrayList<>();

        public AsyncConsumer(String name) {
            this.name = name;
        }

        public void startConsuming(int port) {
            executor.submit(() -> {
                try (Socket socket = new Socket("localhost", port)) {
                    socket.setSoTimeout(1000);
                    DataInputStream stream = new DataInputStream(socket.getInputStream());

                    while (keepReading && !Thread.currentThread().isInterrupted()) {
                        try {
                            if (stream.available() > 0) {
                                int l = stream.readInt();
                                byte[] bytes = stream.readNBytes(l);
                                String result = new String(bytes, StandardCharsets.UTF_8);
                                System.out.println(name + " - Async result: " + result);
                                receivedMessages.add(result);
                            }
                        } catch (SocketTimeoutException e) {
                            continue;
                        }
                    }
                } catch (IOException e) {
                    if (keepReading) {
                        System.err.println(name + " error: " + e.getMessage());
                    }
                }
            });
        }

        public void stop() {
            keepReading = false;
            executor.shutdown();
            try {
                if (!executor.awaitTermination(2, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
            }
        }

        public List<String> getReceivedMessages() {
            return receivedMessages;
        }
    }


    // Helper method to create HAStateManager to assert database
    protected HAStateManager createHAStateManagerForAssertion() {
        HAStateManager manager = HAStateManagerFactory.create(TEST_DB_TYPE);
        manager.initializeHA(HA_UUID, "FOR_ASSERTION", dbParams, dbHAConfig);
        return manager;
    }

    protected String getRuleSetNameValue() {
        return (String) readValueAsMapOfStringAndObject(getRuleSet()).get("name");
    }
}
