package org.drools.ansible.rulebook.integration.main;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.drools.ansible.rulebook.integration.api.ObjectMapperFactory;
import org.drools.ansible.rulebook.integration.core.jpy.AstRulesEngine;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Payload {

    private final List<String> list;

    private int startDelay = 0;

    private int loopCount = 1;
    private int loopDelay = 0;
    private int eventDelay = 0;

    private int shutdown = 0;

    private Payload(List<String> list) {
        this.list = list;
    }

    static Payload parsePayload(Map ruleSet) {
        Map sources = (Map) ((Map) ((List) ruleSet.get("sources")).get(0)).get("EventSource");
        Map sourcesArgs = (Map) sources.get("source_args");
        List<String> payloadList = new ArrayList<>();

        int repeatCount = 1;
        try {
            repeatCount = Integer.valueOf(sourcesArgs.get("repeat_count").toString());
        } catch (Exception e) { /* ignore */ }

        try {
            for (int i = 0; i < repeatCount; i++) {
                for (Object p : (List) sourcesArgs.get("payload")) {
                    payloadList.add(p.toString());
                }
            }
        } catch (Exception e) { /* ignore */ }

        try {
            String indexName = sourcesArgs.get("create_index").toString();
            for (int i = 0; i < repeatCount; i++) {
                String payload = "{\"" + indexName + "\" : " + i + "}";
                payloadList.add(payload);
            }
        } catch (Exception e) { /* ignore */ }

        Payload payload = new Payload(payloadList);

        try {
            payload.eventDelay = Integer.valueOf(sourcesArgs.get("delay").toString());
        } catch (Exception e) {
            try {
            payload.eventDelay = Integer.valueOf(sourcesArgs.get("event_delay").toString());
            } catch (Exception e1) {
                /* ignore */
            }
        }
        try {
            payload.startDelay = Integer.valueOf(sourcesArgs.get("start_delay").toString());
        } catch (Exception e) {
            /* ignore */
        }
        try {
            payload.loopCount = Integer.valueOf(sourcesArgs.get("loop_count").toString());
        } catch (Exception e) {
            /* ignore */
        }
        try {
            payload.loopDelay = Integer.valueOf(sourcesArgs.get("loop_delay").toString());
        } catch (Exception e) {
            /* ignore */
        }
        try {
            payload.shutdown = Integer.valueOf(sourcesArgs.get("loop_delay").toString());
        } catch (Exception e) {
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
                    ObjectMapper mapper = ObjectMapperFactory.createMapper(new JsonFactory());
                    try {
                        returnedMatches.addAll(mapper.readValue(resultJson, List.class));
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
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