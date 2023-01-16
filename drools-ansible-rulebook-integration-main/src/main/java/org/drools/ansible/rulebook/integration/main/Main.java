package org.drools.ansible.rulebook.integration.main;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.drools.ansible.rulebook.integration.core.jpy.AstRulesEngine;
import org.json.JSONArray;
import org.json.JSONObject;

public class Main {

    private static final String DEFAULT_JSON = "timed_out.json";

    public static void main(String[] args) {
        String jsonFile = args.length > 0 ? args[0] : DEFAULT_JSON;

        try (AstRulesEngine engine = new AstRulesEngine();
             InputStream s = Main.class.getClassLoader().getResourceAsStream(jsonFile)) {

            String rules = new String(s.readAllBytes());
            JSONObject ruleSet = (JSONObject) new JSONObject(rules).get("RuleSet");

            long id = engine.createRuleset(ruleSet.toString());
            int port = engine.port();

            Payload payload = Payload.parsePayload(ruleSet);

            try (Socket socket = new Socket("localhost", port)) {
                DataInputStream bufferedInputStream = new DataInputStream(socket.getInputStream());

                long startTime = System.currentTimeMillis();
                payload.execute(engine, id);

                // blocking call
                int l = bufferedInputStream.readInt();
                long firingTime = System.currentTimeMillis() - startTime;

                byte[] bytes = bufferedInputStream.readNBytes(l);
                String r = new String(bytes, StandardCharsets.UTF_8);
                JSONObject v = new JSONObject(r);

                List<Object> matches = v.getJSONArray("result").toList();
                Map<String, Map> match = (Map<String, Map>) matches.get(0);

                System.out.println(match + " fired after " + firingTime + " milliseconds");
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static class Payload {

        private final int delay;

        private final List<String> list;

        private Payload(int delay, List<String> list) {
            this.delay = delay;
            this.list = list;
        }

        private static Payload parsePayload(JSONObject ruleSet) {
            JSONObject sources = (JSONObject) ((JSONObject) ((JSONArray) ruleSet.get("sources")).get(0)).get("EventSource");
            JSONObject sourcesArgs = (JSONObject) sources.get("source_args");
            int delay = Integer.parseInt( sourcesArgs.get("delay").toString() );
            JSONArray payload = (JSONArray) sourcesArgs.get("payload");
            List<String> payloadList = new ArrayList<>();
            for (Object p : payload) {
                payloadList.add(p.toString());
            }

            return new Payload(delay, payloadList);
        }

        public void execute(AstRulesEngine engine, long sessionId) {
            PayloadRunner payloadRunner = new PayloadRunner(this, engine, sessionId);
            Thread thread = new Thread(payloadRunner);
            thread.setDaemon(true);
            thread.start();
        }
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
            for (String p : payload.list) {
                engine.assertEvent(sessionId, p);
                try {
                    Thread.sleep(payload.delay * 1000L);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
