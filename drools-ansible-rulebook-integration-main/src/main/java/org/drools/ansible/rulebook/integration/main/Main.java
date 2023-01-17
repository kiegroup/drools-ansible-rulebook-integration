package org.drools.ansible.rulebook.integration.main;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.drools.ansible.rulebook.integration.core.jpy.AstRulesEngine;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class Main {

    private static final boolean EXECUTE_PAYLOAD_ASYNC = true;

    private static final String DEFAULT_JSON = "once_after.json";

    private static volatile boolean terminated = false;

    public static void main(String[] args) {
        String jsonFile = args.length > 0 ? args[0] : DEFAULT_JSON;

        try (AstRulesEngine engine = new AstRulesEngine()) {

            String rules = readJsonInput(jsonFile);
            JSONObject ruleSet = (JSONObject) new JSONObject(rules).get("RuleSet");

            long id = engine.createRuleset(ruleSet.toString());
            int port = engine.port();

            Payload payload = Payload.parsePayload(ruleSet);

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

        } catch (IOException e) {
            throw new RuntimeException(e);
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

    private static class Payload {

        private final List<String> list;

        private int loopCount = 1;
        private int loopDelay = 0;
        private int delay = 0;

        private Payload(List<String> list) {
            this.list = list;
        }

        private static Payload parsePayload(JSONObject ruleSet) {
            JSONObject sources = (JSONObject) ((JSONObject) ((JSONArray) ruleSet.get("sources")).get(0)).get("EventSource");
            JSONObject sourcesArgs = (JSONObject) sources.get("source_args");
            List<String> payloadList = new ArrayList<>();
            for (Object p : (JSONArray) sourcesArgs.get("payload")) {
                payloadList.add(p.toString());
            }

            Payload payload = new Payload(payloadList);

            try {
                payload.delay = sourcesArgs.getInt("delay");
            } catch (JSONException e) { /* ignore */ }
            try {
                payload.loopCount = sourcesArgs.getInt("loop_count");
            } catch (JSONException e) { /* ignore */ }
            try {
                payload.loopDelay = sourcesArgs.getInt("loop_delay");
            } catch (JSONException e) { /* ignore */ }

            return payload;
        }

        public Runnable asRunnable(AstRulesEngine engine, long sessionId) {
            return new PayloadRunner(this, engine, sessionId);
        }
    }

    private static void executeInNewThread(Runnable runnable) {
        Thread thread = new Thread(runnable);
        thread.setDaemon(true);
        thread.start();
    }

    private static class PayloadRunner implements Runnable {

        private final Payload payload;

        private final AstRulesEngine engine;

        private final long sessionId;

        private PayloadRunner(Payload payload, AstRulesEngine engine, long sessionId) {
            this.payload = payload;
            this.engine = engine;
            this.sessionId = sessionId;
        }

        @Override
        public void run() {
            for (int i = 0; i < payload.loopCount; i++) {
                for (String p : payload.list) {
                    engine.assertEvent(sessionId, p);
                    try {
                        Thread.sleep(payload.delay * 1000L);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
                try {
                    Thread.sleep(payload.loopDelay * 1000L);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
