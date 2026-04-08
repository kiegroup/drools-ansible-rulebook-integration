package org.drools.ansible.rulebook.integration.main;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.drools.ansible.rulebook.integration.api.RuleFormat;
import org.drools.ansible.rulebook.integration.api.RuleNotation;
import org.drools.ansible.rulebook.integration.api.domain.RulesSet;
import org.drools.ansible.rulebook.integration.api.io.JsonMapper;
import org.drools.ansible.rulebook.integration.core.jpy.AstRulesEngine;
import org.drools.ansible.rulebook.integration.main.Payload.PayloadRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);
    
    private static final boolean EXECUTE_PAYLOAD_ASYNC = true;

    private static final String DEFAULT_JSON = "test_accumulate_within_ast.json";

    private static final int THREADS_NR = 1; // run with 1 thread by default

    private static final int EXPECTED_MATCHES = -1; // expected number of matches, negative to ignore

    private static volatile boolean terminated = false;

    private static boolean foundError = false;

    private static AtomicLong usedMemory = new AtomicLong();
    private static AtomicLong timeTaken = new AtomicLong();

    public static void main(String[] args) throws InterruptedException {
        boolean haEnabled = false;
        String haDbParamsJson = null;
        String haUuid = null;
        boolean failoverRecovery = false;
        List<String> positionalArgs = new ArrayList<>();
        for (int i = 0; i < args.length; i++) {
            if ("--ha".equals(args[i])) {
                haEnabled = true;
            } else if ("--ha-db-params".equals(args[i])) {
                haEnabled = true;
                if (i + 1 < args.length) {
                    haDbParamsJson = args[++i];
                } else {
                    System.err.println("ERROR: --ha-db-params requires a JSON argument");
                    System.exit(1);
                }
            } else if ("--ha-uuid".equals(args[i])) {
                if (i + 1 < args.length) {
                    haUuid = args[++i];
                } else {
                    System.err.println("ERROR: --ha-uuid requires a value");
                    System.exit(1);
                }
            } else if ("--failover-recovery".equals(args[i])) {
                haEnabled = true;
                failoverRecovery = true;
            } else {
                positionalArgs.add(args[i]);
            }
        }
        String jsonFile = positionalArgs.isEmpty() ? DEFAULT_JSON : positionalArgs.get(0);
        parallelExecute(jsonFile, haEnabled, haDbParamsJson, haUuid, failoverRecovery);

        // for test script convenience, print the information about the execution to STDERR
        StringBuilder sb = new StringBuilder();
        sb.append(jsonFile);
        if (failoverRecovery) {
            sb.append(" (failover-recovery)");
        } else if (haDbParamsJson != null) {
            sb.append(" (HA-custom)");
        } else if (haEnabled) {
            sb.append(" (HA)");
        }
        sb.append(", ")
                .append(usedMemory.get()) // bytes
                .append(", ")
                .append(timeTaken.get()); // milliseconds
        System.err.println(sb.toString());
    }

    private static void parallelExecute(String jsonFile, boolean haEnabled, String haDbParamsJson, String haUuid, boolean failoverRecovery) throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(THREADS_NR);

        for (int n = 0; n < THREADS_NR; n++) {
            executor.execute(() -> {
                ExecuteResult result = execute(jsonFile, haEnabled, haDbParamsJson, haUuid, failoverRecovery);
                LOGGER.info("Executed in " + result.getDuration() + " msecs");
            });
        }

        executor.shutdown();
        executor.awaitTermination(300, TimeUnit.SECONDS);

        if (foundError) {
            System.err.println("ERROR FOUND!!! Check above logs");
            throw new IllegalStateException();
        }
    }

    public static ExecuteResult execute(String jsonFile) {
        return execute(jsonFile, false, null, null, false);
    }

    public static ExecuteResult execute(String jsonFile, boolean haEnabled, String haDbParamsJson, String haUuid, boolean failoverRecovery) {
        if (haEnabled && haDbParamsJson == null) {
            cleanH2Files();
        }
        try (AstRulesEngine engine = new AstRulesEngine()) {
            Map jsonRuleSet = getJsonRuleSet(jsonFile);
            Payload payload = Payload.parsePayload(jsonRuleSet);

            final String JSON_OF_JSONRULESET = JsonMapper.toJson(jsonRuleSet);

            RulesSet rulesSet = RuleNotation.CoreNotation.INSTANCE.toRulesSet(RuleFormat.JSON, JSON_OF_JSONRULESET);

            if (failoverRecovery) {
                return executeHAFailoverRecovery(engine, rulesSet, JSON_OF_JSONRULESET, haDbParamsJson, haUuid);
            }

            if (haEnabled) {
                return executeHA(engine, rulesSet, JSON_OF_JSONRULESET, payload, haDbParamsJson, haUuid);
            }

            // Non-HA path (original)
            long id = engine.createRuleset(rulesSet);
            int port = engine.port();

            LOGGER.info("*** Start measuring execution time");
            Instant start = Instant.now();
            List<Map> returnedMatches = executePayload(engine, rulesSet, id, port, payload);
            long duration = Duration.between(start, Instant.now()).toMillis();
            LOGGER.info("*** End measuring execution time , duration = {} ms", duration);

            String stats = engine.sessionStats(id);
            LOGGER.info(stats);

            payload = null; // allow GC to collect the payload object because it can be large
            timeTaken.set(duration);
            System.gc();
            try {
                Thread.sleep(1000); // this sleep gives relatively stable gc results
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            System.gc();
            // Need to check usedMemory before disposing the engine
            usedMemory.set(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory());

            return new ExecuteResult(returnedMatches, duration);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static ExecuteResult executeHA(AstRulesEngine engine, RulesSet rulesSet, String rulesetJson, Payload payload, String customDbParamsJson, String customHaUuid) {
        // 1. Initialize HA (also calls allowAsync() internally)
        String haUuid = customHaUuid != null ? customHaUuid : "loadtest-ha-" + System.currentTimeMillis();
        String dbParamsJson = customDbParamsJson != null ? customDbParamsJson : "{\"db_type\":\"h2\",\"db_file_path\":\"./target/loadtest_ha_db\"}";
        String configJson = "{\"write_after\":1}";
        engine.initializeHA(haUuid, "loadtest-worker", dbParamsJson, configJson);

        // 2. Create ruleset with rulesetString (required for HA session recovery)
        long id = engine.createRuleset(rulesSet, rulesetJson);
        int port = engine.port();

        // 3. Connect socket (required for isConnected() checks in HA mode)
        Socket haSocket;
        try {
            haSocket = new Socket("localhost", port);
        } catch (IOException e) {
            throw new RuntimeException("Failed to connect HA socket", e);
        }

        try {
            // 4. Enable leader
            engine.enableLeader();

            LOGGER.info("*** Start measuring execution time");
            Instant start = Instant.now();
            List<Map> returnedMatches = payload.execute(engine, id);
            long duration = Duration.between(start, Instant.now()).toMillis();
            LOGGER.info("*** End measuring execution time , duration = {} ms", duration);

            if (EXPECTED_MATCHES >= 0 && returnedMatches.size() != EXPECTED_MATCHES) {
                LOGGER.error("Unexpected number of matches, expected = " + EXPECTED_MATCHES + " actual = " + returnedMatches.size());
                foundError = true;
            }

            String stats = engine.sessionStats(id);
            LOGGER.info(stats);

            payload = null;
            timeTaken.set(duration);
            System.gc();
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            System.gc();
            usedMemory.set(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory());

            return new ExecuteResult(returnedMatches, duration);
        } finally {
            try {
                haSocket.close();
            } catch (IOException e) {
                LOGGER.warn("Failed to close HA socket", e);
            }
        }
    }

    private static ExecuteResult executeHAFailoverRecovery(AstRulesEngine engine, RulesSet rulesSet, String rulesetJson, String customDbParamsJson, String haUuid) {
        if (haUuid == null) {
            throw new IllegalArgumentException("--ha-uuid is required for --failover-recovery mode");
        }
        if (customDbParamsJson == null) {
            throw new IllegalArgumentException("--ha-db-params is required for --failover-recovery mode");
        }

        // 1. Initialize HA with the same UUID as the loading node
        String configJson = "{\"write_after\":1}";
        engine.initializeHA(haUuid, "recovery-worker", customDbParamsJson, configJson);

        // 2. Create ruleset (required for recovery to rebuild KieSession)
        long id = engine.createRuleset(rulesSet, rulesetJson);
        int port = engine.port();

        // 3. Connect socket (required for isConnected() checks in HA mode)
        Socket haSocket;
        try {
            haSocket = new Socket("localhost", port);
        } catch (IOException e) {
            throw new RuntimeException("Failed to connect HA socket", e);
        }

        try {
            // 4. Measure enableLeader() — this is the recovery
            LOGGER.info("*** Start measuring recovery time (enableLeader)");
            Instant start = Instant.now();
            engine.enableLeader();
            long recoveryDuration = Duration.between(start, Instant.now()).toMillis();
            LOGGER.info("*** Recovery completed in {} ms", recoveryDuration);

            String stats = engine.sessionStats(id);
            LOGGER.info(stats);

            // 5. Measure memory
            timeTaken.set(recoveryDuration);
            System.gc();
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            System.gc();
            usedMemory.set(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory());

            return new ExecuteResult(List.of(), recoveryDuration);
        } finally {
            try {
                haSocket.close();
            } catch (IOException e) {
                LOGGER.warn("Failed to close HA socket", e);
            }
        }
    }

    private static final String H2_DB_PATH = "./target/loadtest_ha_db";

    private static void cleanH2Files() {
        new File(H2_DB_PATH + ".mv.db").delete();
        new File(H2_DB_PATH + ".trace.db").delete();
    }

    private static Map getJsonRuleSet(String jsonFile) {
        String rules = readJsonInput(jsonFile);
        Map jsonObject = rules.startsWith("[") ? (Map) JsonMapper.readValueAsListOfObject(rules).get(0) : JsonMapper.readValueAsMapOfStringAndObject(rules);
        return (Map) jsonObject.get("RuleSet");
    }

    private static List<Map> executePayload(AstRulesEngine engine, RulesSet rulesSet, long id, int port, Payload payload) throws IOException {
        if (rulesSet.hasAsyncExecution()) {
            return runAsyncExec(engine, id, port, payload);
        } else {
            List<Map> returnedMatches = payload.execute(engine, id);
            //LOGGER.info("Returned matches: " + returnedMatches.size());
            //returnedMatches.forEach(map -> LOGGER.info("  " + map.entrySet()));

            if (EXPECTED_MATCHES >= 0 && returnedMatches.size() != EXPECTED_MATCHES) {
                LOGGER.error("Unexpected number of matches, expected = " + EXPECTED_MATCHES + " actual = " + returnedMatches.size());
                foundError = true;
            }
            return returnedMatches;
        }
    }

    private static List<Map> runAsyncExec(AstRulesEngine engine, long id, int port, Payload payload) throws IOException {
        if (payload.getStartDelay() > 0) {
            try {
                Thread.sleep( payload.getStartDelay() * 1000L );
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        try (Socket socket = new Socket("localhost", port)) {
            if (EXECUTE_PAYLOAD_ASYNC) {
                executeInNewThread(payload.asRunnable(engine, id));
                return readAsyncChannel(socket);
            } else {
                executeInNewThread(() -> readAsyncChannel(socket));
                PayloadRunner payloadRunnable = (PayloadRunner) payload.asRunnable(engine, id);
                payloadRunnable.run();
                waitForTermination();
                return payloadRunnable.getReturnedMatches();
            }
        }
    }

    private static void waitForTermination() {
        while (!terminated) {
            try {
                Thread.sleep(100L);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static List<Map> readAsyncChannel(Socket socket) {
        try {
            DataInputStream bufferedInputStream = new DataInputStream(socket.getInputStream());
            long startTime = System.currentTimeMillis();

            // blocking call
            int nBytes = bufferedInputStream.readInt();
            long firingTime = System.currentTimeMillis() - startTime;

            byte[] bytes = bufferedInputStream.readNBytes(nBytes);
            String r = new String(bytes, StandardCharsets.UTF_8);

            List<Object> matches = JsonMapper.readValueExtractFieldAsList(r, "result");
            Map<String, Map> match = (Map<String, Map>) matches.get(0);

            LOGGER.info(match + " fired after " + firingTime + " milliseconds");

            List<Map> matchMapList = matches.stream().map(Map.class::cast).collect(Collectors.toList());

            terminated = true;
            return matchMapList;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static String readJsonInput(String jsonFile) {
        try (InputStream is1 = Main.class.getClassLoader().getResourceAsStream(jsonFile)) {
            return new String(is1.readAllBytes(), StandardCharsets.UTF_8);
        } catch (Exception e) {
            try (InputStream is2 = new FileInputStream(jsonFile)) {
                return new String(is2.readAllBytes(), StandardCharsets.UTF_8);
            } catch (FileNotFoundException ex) {
                throw new RuntimeException(ex);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    private static void executeInNewThread(Runnable runnable) {
        Thread thread = new Thread(runnable);
        thread.setDaemon(true);
        thread.start();
    }

    public static class ExecuteResult {
        private final List<Map> returnedMatches;
        private final long duration;

        public ExecuteResult(List<Map> returnedMatches, long duration) {
            this.returnedMatches = returnedMatches;
            this.duration = duration;
        }

        public List<Map> getReturnedMatches() {
            return returnedMatches;
        }

        public long getDuration() {
            return duration;
        }
    }
}
