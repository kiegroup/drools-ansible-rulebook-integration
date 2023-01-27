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

import org.drools.ansible.rulebook.integration.api.RuleFormat;
import org.drools.ansible.rulebook.integration.api.RuleNotation;
import org.drools.ansible.rulebook.integration.api.domain.RulesSet;
import org.drools.ansible.rulebook.integration.core.jpy.AstRulesEngine;
import org.json.JSONArray;
import org.json.JSONObject;

public class Main {

    private static final boolean EXECUTE_PAYLOAD_ASYNC = true;

    private static final String DEFAULT_JSON = "1k_event_rules_ast.json";

    private static volatile boolean terminated = false;

    public static void main(String[] args) {
        String jsonFile = args.length > 0 ? args[0] : DEFAULT_JSON;

        try (AstRulesEngine engine = new AstRulesEngine()) {
            JSONObject jsonRuleSet = getJsonRuleSet(jsonFile);
            Payload payload = Payload.parsePayload(jsonRuleSet);

            RulesSet rulesSet = RuleNotation.CoreNotation.INSTANCE.toRulesSet(RuleFormat.JSON, jsonRuleSet.toString());
            long id = engine.createRuleset(rulesSet);
            int port = engine.port();

            Instant start = Instant.now();
            executePayload(engine, rulesSet, id, port, payload);
            long duration = Duration.between(start, Instant.now()).toMillis();
            System.out.println("Executed in " + duration + " msecs");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static JSONObject getJsonRuleSet(String jsonFile) {
        String rules = readJsonInput(jsonFile);
        JSONObject jsonObject = rules.startsWith("[") ? (JSONObject) new JSONArray(rules).get(0) : new JSONObject(rules);
        return (JSONObject) jsonObject.get("RuleSet");
    }

    private static void executePayload(AstRulesEngine engine, RulesSet rulesSet, long id, int port, Payload payload) throws IOException {
        if (rulesSet.hasAsyncExecution()) {
            runAsyncExec(engine, id, port, payload);
        } else {
            payload.asRunnable(engine, id).run();
        }
    }

    private static void runAsyncExec(AstRulesEngine engine, long id, int port, Payload payload) throws IOException {
        try (Socket socket = new Socket("localhost", port)) {
            if (EXECUTE_PAYLOAD_ASYNC) {
                executeInNewThread(payload.asRunnable(engine, id));
                readAsyncChannel(socket);
            } else {
                executeInNewThread(() -> readAsyncChannel(socket));
                payload.asRunnable(engine, id).run();
            }
            waitForTermination();
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

    private static void readAsyncChannel(Socket socket) {
        try {
            DataInputStream bufferedInputStream = new DataInputStream(socket.getInputStream());
            long startTime = System.currentTimeMillis();

            // blocking call
            int nBytes = bufferedInputStream.readInt();
            long firingTime = System.currentTimeMillis() - startTime;

            byte[] bytes = bufferedInputStream.readNBytes(nBytes);
            String r = new String(bytes, StandardCharsets.UTF_8);
            JSONObject v = new JSONObject(r);

            List<Object> matches = v.getJSONArray("result").toList();
            Map<String, Map> match = (Map<String, Map>) matches.get(0);

            System.out.println(match + " fired after " + firingTime + " milliseconds");

            terminated = true;
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
}
