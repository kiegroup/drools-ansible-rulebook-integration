package org.drools.ansible.rulebook.integration.main;

import java.util.ArrayList;
import java.util.List;

import org.drools.ansible.rulebook.integration.core.jpy.AstRulesEngine;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class Payload {

    private final List<String> list;

    private int loopCount = 1;
    private int loopDelay = 0;
    private int delay = 0;

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
                    sleep(payload.delay);
                }
                sleep(payload.loopDelay);
            }
        }

        private void sleep(int secondsToSleep) {
            if (secondsToSleep > 0) {
                try {
                    Thread.sleep(secondsToSleep * 1000L);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

}