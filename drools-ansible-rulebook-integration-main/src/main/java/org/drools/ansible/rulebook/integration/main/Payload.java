package org.drools.ansible.rulebook.integration.main;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.drools.ansible.rulebook.integration.api.ObjectMapperFactory;
import org.drools.ansible.rulebook.integration.core.jpy.AstRulesEngine;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Payload {

    private final List<String> list;

    private int loopCount = 1;
    private int loopDelay = 0;
    private int delay = 0;

    private int shutdown = 0;

    private Payload(List<String> list) {
        this.list = list;
    }

    static Payload parsePayload(JSONObject ruleSet) {
        JSONObject sources = (JSONObject) ((JSONObject) ((JSONArray) ruleSet.get("sources")).get(0)).get("EventSource");
        JSONObject sourcesArgs = (JSONObject) sources.get("source_args");
        List<String> payloadList = new ArrayList<>();

        int repeatCount = 1;
        try {
            repeatCount = sourcesArgs.getInt("repeat_count");
        } catch (JSONException e) { /* ignore */ }

        try {
            for (int i = 0; i < repeatCount; i++) {
                for (Object p : (JSONArray) sourcesArgs.get("payload")) {
                    payloadList.add(p.toString());
                }
            }
        } catch (JSONException e) { /* ignore */ }

        try {
            String indexName = sourcesArgs.getString("create_index");
            for (int i = 0; i < repeatCount; i++) {
                String payload = "{\"" + indexName + "\" : " + i + "}";
                payloadList.add(payload);
            }
        } catch (JSONException e) { /* ignore */ }

        Payload payload = new Payload(payloadList);

        try {
            payload.delay = sourcesArgs.getInt("delay");
        } catch (JSONException e) {
            try {
            payload.delay = sourcesArgs.getInt("event_delay");
            } catch (JSONException e1) {
                /* ignore */
            }
        }
        try {
            payload.loopCount = sourcesArgs.getInt("loop_count");
        } catch (JSONException e) {
            /* ignore */
        }
        try {
            payload.loopDelay = sourcesArgs.getInt("loop_delay");
        } catch (JSONException e) {
            /* ignore */
        }
        try {
            payload.shutdown = sourcesArgs.getInt("loop_delay");
        } catch (JSONException e) {
            /* ignore */
        }

        return payload;
    }

    public Runnable asRunnable(AstRulesEngine engine, long sessionId) {
        return new PayloadRunner(this, engine, sessionId);
    }

    public List<Map> execute(AstRulesEngine engine, long sessionId) {
        PayloadRunner payloadRunner = new PayloadRunner(this, engine, sessionId);
        payloadRunner.run();
        return payloadRunner.getReturnedMatches();
    }

    private static class PayloadRunner implements Runnable {

        private final Payload payload;

        private final AstRulesEngine engine;

        private final long sessionId;

        private List<Map> returnedMatches = new ArrayList<>();

        private PayloadRunner(Payload payload, AstRulesEngine engine, long sessionId) {
            this.payload = payload;
            this.engine = engine;
            this.sessionId = sessionId;
        }

        @Override
        public void run() {
//            try {
//                Thread.sleep(1300);
//            } catch (InterruptedException e) {
//                throw new RuntimeException(e);
//            }
            long start = System.currentTimeMillis();
            for (int i = 0; i < payload.loopCount; i++) {
                for (String p : payload.list) {
                    String resultJson = engine.assertEvent(sessionId, p);
                    ObjectMapper mapper = ObjectMapperFactory.createMapper(new JsonFactory());
                    try {
                        returnedMatches.addAll(mapper.readValue(resultJson, List.class));
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                    sleepSeconds(payload.delay);
                }
                sleepSeconds(payload.loopDelay);
            }
            long elapsed = System.currentTimeMillis() - start;
            sleepMillis( (payload.shutdown * 1000L) - elapsed );
        }

        private void sleepSeconds(int secondsToSleep) {
            sleepMillis(secondsToSleep * 1000L);
        }

        private void sleepMillis(long millisToSleep) {
            if (millisToSleep > 0) {
                try {
                    Thread.sleep(millisToSleep);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        public List<Map> getReturnedMatches() {
            return returnedMatches;
        }
    }
}