package org.drools.ansible.rulebook.integration.core.jpy;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.drools.ansible.rulebook.integration.api.RuleConfigurationOption;
import org.drools.ansible.rulebook.integration.api.RuleFormat;
import org.drools.ansible.rulebook.integration.api.RuleNotation;
import org.drools.ansible.rulebook.integration.api.RulesExecutorContainer;
import org.drools.ansible.rulebook.integration.api.domain.RulesSet;
import org.json.JSONObject;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.StandardSocketOptions;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.drools.ansible.rulebook.integration.api.RulesExecutor.OBJECT_MAPPER;

public class AsyncAstRulesEngine {
    static class Response {
        private final long session_id;
        private final Object result;

        public Response(long session_id, Object result) {
            this.session_id = session_id;
            this.result = result;
        }

        public long getSession_id() {
            return session_id;
        }

        public Object getResult() {
            return result;
        }
    }

    private final ServerSocket socketChannel;
    private final AstRulesEngineInternal astRulesEngine;
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private volatile DataOutputStream dataOutputStream;
    private boolean shutdown = false;

    public AsyncAstRulesEngine() {
        try {
            astRulesEngine = new AstRulesEngineInternal();
            socketChannel = new ServerSocket(0);
            socketChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
            accept();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public int port() {
        return socketChannel.getLocalPort();
    }

    private void accept() {
        executor.submit(() -> {
            try {
                Socket skt = socketChannel.accept();
                this.dataOutputStream = new DataOutputStream(skt.getOutputStream());
                return skt;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

    }

    public long createRuleset(String rulesetString) {
        if (shutdown) throw new IllegalStateException("This AsyncAstRulesEngine is shutting down");
        RulesSet rulesSet = RuleNotation.CoreNotation.INSTANCE.toRulesSet(RuleFormat.JSON, rulesetString);
        long sessionId = astRulesEngine.createRuleset(rulesSet);
        return sessionId;
    }

    public long createRulesetWithOptions(String rulesetString, boolean pseudoClock) {
        if (shutdown) throw new IllegalStateException("This AsyncAstRulesEngine is shutting down");
        RulesSet rulesSet = RuleNotation.CoreNotation.INSTANCE.toRulesSet(RuleFormat.JSON, rulesetString);
        if (pseudoClock) rulesSet.withOptions(RuleConfigurationOption.USE_PSEUDO_CLOCK);
        long sessionId = astRulesEngine.createRuleset(rulesSet);
        return sessionId;
    }

    public void dispose(long sessionId) {
        astRulesEngine.dispose(sessionId);
    }

    public void retractFact(long sessionId, String serializedFact) {
        Map<String, Object> fact = new JSONObject(serializedFact).toMap();
        List<Map<String, ?>> retractResult = astRulesEngine.retractFact(sessionId, fact);
        if (retractResult.isEmpty()) return; // skip empty result
        write(new Response(sessionId, retractResult));
    }

    public void assertFact(long sessionId, String serializedFact) {
        Map<String, Object> fact = new JSONObject(serializedFact).toMap();
        List<Map<String, Map>> assertResult = astRulesEngine.assertFact(sessionId, fact);
        if (assertResult.isEmpty()) return; // skip empty result
        write(new Response(sessionId, assertResult));
    }

    public void assertEvent(long sessionId, String serializedFact) {
        Map<String, Object> fact = new JSONObject(serializedFact).toMap();
        List<Map<String, Map>> assertResult = astRulesEngine.assertEvent(sessionId, fact);
        if (assertResult.isEmpty()) return; // skip empty result
        write(new Response(sessionId, assertResult));
    }

    public String getFacts(long sessionId) {
        return toJson(astRulesEngine.getFacts(sessionId));
    }

    public void advanceTime(long sessionId, long amount, String unit) {
        List<Map<String, Map>> matches = AstRuleMatch.asList(astRulesEngine.advanceTime(sessionId, amount, unit));
        if (matches.isEmpty()) return; // skip empty result
        write(new Response(sessionId, matches));
    }

    public void shutdown() {
        shutdown = true;
        RulesExecutorContainer.INSTANCE.disposeAll();
        executor.shutdown();
    }

    private void write(Response response) {
        executor.submit(() -> {
            try {
                String payload = toJson(response);
                byte[] bytes = payload.getBytes(StandardCharsets.UTF_8);
                dataOutputStream.writeInt(bytes.length);
                dataOutputStream.write(bytes);
                dataOutputStream.flush();
            } catch (IOException | UncheckedIOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private static String toJson(Object object) {
        try {
            return OBJECT_MAPPER.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }
}
