package org.drools.ansible.rulebook.integration.main;

import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.drools.ansible.rulebook.integration.api.io.JsonMapper;
import org.drools.ansible.rulebook.integration.core.jpy.AstRulesEngine;

public class Payload {

    private final List<String> list;

    private int startDelay = 0;

    private int loopCount = 1;
    private int loopDelay = 0;
    private int eventDelay = 0;

    private int shutdown = 0;

    // set true when matchedEvents occupies too much memory
    private boolean discardMatchedEvents = false;

    private Payload(List<String> list) {
        this.list = list;
    }

    static Payload parsePayload(Map ruleSet) {
        Map eventsource = (Map) ((Map) ((List) ruleSet.get("sources")).get(0)).get("EventSource");
        if (!eventsource.get("source_name").equals("generic")) {
            // this class currently mimics behaviour of: https://github.com/ansible/event-driven-ansible/blob/main/extensions/eda/plugins/event_source/generic.py
            // as such we should fail if the provided AST file (json variant) does not contain a `generic` EDA source as the sources[0].
            throw new IllegalArgumentException("Was expecting EventSource to be of type generic, found instead: "+eventsource);
        }
        Map sourcesArgs = (Map) eventsource.get("source_args");
        List<String> payloadList = new ArrayList<>();

        int repeatCount = 1;
        try {
            repeatCount = Integer.valueOf(sourcesArgs.get("repeat_count").toString());
        } catch (NullPointerException | NumberFormatException e) { /* ignore */ }

        try {
            List<String> payloadJsonCache = new ArrayList<>(); // reuse the same String instances to avoid memory consumption on the client side
            for (Object p : (List) sourcesArgs.get("payload")) {
                payloadJsonCache.add(JsonMapper.toJson(p));
            }
            for (int i = 0; i < repeatCount; i++) {
                for (String payloadJson : payloadJsonCache) {
                    payloadList.add(payloadJson);
                }
            }
        } catch (UncheckedIOException e) { /* ignore */ }

        try {
            String indexName = sourcesArgs.get("create_index").toString();
            for (int i = 0; i < repeatCount; i++) {
                String payload = "{\"" + indexName + "\" : " + i + "}";
                payloadList.add(payload);
            }
        } catch (NullPointerException e) { /* ignore */ }

        Payload payload = new Payload(payloadList);

        try {
            payload.eventDelay = Integer.valueOf(sourcesArgs.get("delay").toString());
        } catch (NullPointerException | NumberFormatException e) {
            try {
            payload.eventDelay = Integer.valueOf(sourcesArgs.get("event_delay").toString());
            } catch (NullPointerException | NumberFormatException e1) {
                /* ignore */
            }
        }
        try {
            payload.startDelay = Integer.valueOf(sourcesArgs.get("start_delay").toString());
        } catch (NullPointerException | NumberFormatException e) {
            /* ignore */
        }
        try {
            payload.loopCount = Integer.valueOf(sourcesArgs.get("loop_count").toString());
        } catch (NullPointerException | NumberFormatException e) {
            /* ignore */
        }
        try {
            payload.loopDelay = Integer.valueOf(sourcesArgs.get("loop_delay").toString());
        } catch (NullPointerException | NumberFormatException e) {
            /* ignore */
        }
        try {
            payload.shutdown = Integer.valueOf(sourcesArgs.get("loop_delay").toString());
        } catch (NullPointerException | NumberFormatException e) {
            /* ignore */
        }
        try {
            payload.discardMatchedEvents = Boolean.valueOf(sourcesArgs.get("discard_matched_events").toString());
        } catch (NullPointerException | NumberFormatException e) {
            /* ignore */
        }

        return payload;
    }

    public int getStartDelay() {
        return startDelay;
    }

    public Runnable asRunnable(AstRulesEngine engine, long sessionId) {
        return new PayloadRunner(this, engine, sessionId);
    }

    public List<Map> execute(AstRulesEngine engine, long sessionId) {
        PayloadRunner payloadRunner = new PayloadRunner(this, engine, sessionId);
        payloadRunner.run();
        return payloadRunner.getReturnedMatches();
    }

    public static class PayloadRunner implements Runnable {

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
            long start = System.currentTimeMillis();
            for (int i = 0; i < payload.loopCount; i++) {
                for (String p : payload.list) {
                    String resultJson = engine.assertEvent(sessionId, p);
                    if (!payload.discardMatchedEvents) {
                        returnedMatches.addAll(JsonMapper.readValueAsListOfMapOfStringAndObject(resultJson));
                    }
                    sleepSeconds(payload.eventDelay);
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