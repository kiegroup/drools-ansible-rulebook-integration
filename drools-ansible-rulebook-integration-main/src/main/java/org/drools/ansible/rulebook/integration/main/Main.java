package org.drools.ansible.rulebook.integration.main;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
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
        String jsonFile = args.length > 0 ? args[0] : DEFAULT_JSON;
        parallelExecute(jsonFile);

        // for test script convenience, print the information about the execution to STDERR
        StringBuilder sb = new StringBuilder();
        sb.append(jsonFile)
                .append(", ")
                .append(usedMemory.get()) // bytes
                .append(", ")
                .append(timeTaken.get()); // milliseconds
        System.err.println(sb.toString());
    }

    private static void parallelExecute(String jsonFile) throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(THREADS_NR);

        for (int n = 0; n < THREADS_NR; n++) {
            executor.execute(() -> {
                ExecuteResult result = execute(jsonFile);
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
        try (AstRulesEngine engine = new AstRulesEngine()) {
            Map jsonRuleSet = getJsonRuleSet(jsonFile);
            Payload payload = Payload.parsePayload(jsonRuleSet);

            final String JSON_OF_JSONRULESET = JsonMapper.toJson(jsonRuleSet);

            RulesSet rulesSet = RuleNotation.CoreNotation.INSTANCE.toRulesSet(RuleFormat.JSON, JSON_OF_JSONRULESET);
            long id = engine.createRuleset(rulesSet);
            int port = engine.port();

            Instant start = Instant.now();
            List<Map> returnedMatches = executePayload(engine, rulesSet, id, port, payload);

            String stats = engine.sessionStats(id);
            LOGGER.info(stats);

            long duration = Duration.between(start, Instant.now()).toMillis();

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
            LOGGER.info("Returned matches: " + returnedMatches.size());
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
