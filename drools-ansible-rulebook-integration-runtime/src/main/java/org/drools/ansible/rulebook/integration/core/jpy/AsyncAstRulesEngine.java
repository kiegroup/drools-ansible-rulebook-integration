package org.drools.ansible.rulebook.integration.core.jpy;

import org.drools.ansible.rulebook.integration.api.RuleFormat;
import org.drools.ansible.rulebook.integration.api.RuleNotation;
import org.drools.ansible.rulebook.integration.api.domain.RulesSet;
import org.json.JSONObject;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.StandardSocketOptions;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
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
    private volatile DataOutputStream ssc;

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
        CompletableFuture.supplyAsync(() -> {
            try {
                Socket skt = socketChannel.accept();
                this.ssc = new DataOutputStream(skt.getOutputStream());
                return skt;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

    }

    public long createRuleset(String rulesetString) {
        RulesSet rulesSet = RuleNotation.CoreNotation.INSTANCE.toRulesSet(RuleFormat.JSON, rulesetString);
        return astRulesEngine.createRuleset(rulesSet);
    }

    public void dispose(long sessionId) {
        astRulesEngine.dispose(sessionId);
    }

    public void retractFact(long sessionId, String serializedFact) {
        Map<String, Object> fact = new JSONObject(serializedFact).toMap();
        List<Map<String, ?>> retractResult = astRulesEngine.retractFact(sessionId, fact);
        write(new Response(sessionId, retractResult));
    }

    public void assertFact(long sessionId, String serializedFact) {
        Map<String, Object> fact = new JSONObject(serializedFact).toMap();
        List<Map<String, Map>> assertResult = astRulesEngine.assertFact(sessionId, fact);
        write(new Response(sessionId, assertResult));
    }

    public void assertEvent(long sessionId, String serializedFact) {
        Map<String, Object> fact = new JSONObject(serializedFact).toMap();
        List<Map<String, Map>> assertResult = astRulesEngine.assertEvent(sessionId, fact);
        write(new Response(sessionId, assertResult));
    }

    public void getFacts(long session_id) {
        List<Map<String, Object>> facts = astRulesEngine.getFacts(session_id);
        write(new Response(session_id, facts));
    }

    private void write(Response response) {
        executor.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    if (ssc == null) {
                        executor.submit(this);
                        return;
                    }
                    String payload = OBJECT_MAPPER.writeValueAsString(response);
                    byte[] bytes = payload.getBytes(StandardCharsets.UTF_8);
                    ssc.writeInt(bytes.length);
                    ssc.write(bytes);
                    ssc.flush();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }
}
