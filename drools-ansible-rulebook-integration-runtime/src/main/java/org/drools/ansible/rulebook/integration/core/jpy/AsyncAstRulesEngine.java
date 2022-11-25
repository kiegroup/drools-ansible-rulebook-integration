package org.drools.ansible.rulebook.integration.core.jpy;

import java.util.List;
import java.util.Map;

import org.drools.ansible.rulebook.integration.api.RuleConfigurationOption;
import org.drools.ansible.rulebook.integration.api.RuleFormat;
import org.drools.ansible.rulebook.integration.api.RuleNotation;
import org.drools.ansible.rulebook.integration.api.RulesExecutorContainer;
import org.drools.ansible.rulebook.integration.api.domain.RulesSet;
import org.drools.ansible.rulebook.integration.api.io.Response;
import org.drools.ansible.rulebook.integration.api.io.RuleExecutorChannel;
import org.json.JSONObject;

import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.toJson;

public class AsyncAstRulesEngine {

    private final RuleExecutorChannel channel = new RuleExecutorChannel();
    private final AstRulesEngineInternal astRulesEngine = new AstRulesEngineInternal();
    private boolean shutdown = false;

    public int port() {
        // used by client to know on which port the socket has been opened
        return channel.port();
    }

    public long createRuleset(String rulesetString) {
        checkAlive();
        RulesSet rulesSet = RuleNotation.CoreNotation.INSTANCE.toRulesSet(RuleFormat.JSON, rulesetString);
        long sessionId = astRulesEngine.createRuleset(rulesSet);
        return sessionId;
    }

    public long createRulesetWithOptions(String rulesetString, boolean pseudoClock) {
        checkAlive();
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
        channel.write(new Response(sessionId, retractResult));
    }

    public void assertFact(long sessionId, String serializedFact) {
        Map<String, Object> fact = new JSONObject(serializedFact).toMap();
        List<Map<String, Map>> assertResult = astRulesEngine.assertFact(sessionId, fact);
        if (assertResult.isEmpty()) return; // skip empty result
        channel.write(new Response(sessionId, assertResult));
    }

    public void assertEvent(long sessionId, String serializedFact) {
        Map<String, Object> fact = new JSONObject(serializedFact).toMap();
        List<Map<String, Map>> assertResult = astRulesEngine.assertEvent(sessionId, fact);
        if (assertResult.isEmpty()) return; // skip empty result
        channel.write(new Response(sessionId, assertResult));
    }

    public String getFacts(long sessionId) {
        return toJson(astRulesEngine.getFacts(sessionId));
    }

    public void advanceTime(long sessionId, long amount, String unit) {
        List<Map<String, Map>> matches = AstRuleMatch.asList(astRulesEngine.advanceTime(sessionId, amount, unit));
        if (matches.isEmpty()) return; // skip empty result
        channel.write(new Response(sessionId, matches));
    }

    public void shutdown() {
        shutdown = true;
        RulesExecutorContainer.INSTANCE.disposeAll();
        channel.shutdown();
    }

    private void checkAlive() {
        if (shutdown) {
            throw new IllegalStateException("This AsyncAstRulesEngine is shutting down");
        }
    }
}
