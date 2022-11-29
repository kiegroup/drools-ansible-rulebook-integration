package org.drools.ansible.rulebook.integration.core.jpy;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

import org.drools.ansible.rulebook.integration.api.RuleConfigurationOption;
import org.drools.ansible.rulebook.integration.api.RuleFormat;
import org.drools.ansible.rulebook.integration.api.RuleNotation;
import org.drools.ansible.rulebook.integration.api.RulesExecutor;
import org.drools.ansible.rulebook.integration.api.RulesExecutorContainer;
import org.drools.ansible.rulebook.integration.api.RulesExecutorFactory;
import org.drools.ansible.rulebook.integration.api.domain.RulesSet;
import org.json.JSONObject;

import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.toJson;

public class AsyncAstRulesEngine {

    private final RulesExecutorContainer rulesExecutorContainer = new RulesExecutorContainer().allowAsync();

    private boolean shutdown = false;

    public int port() {
        return rulesExecutorContainer.port();
    }

    public long createRuleset(String rulesetString) {
        return createRulesetWithOptions(rulesetString, false);
    }

    public long createRulesetWithOptions(String rulesetString, boolean pseudoClock) {
        checkAlive();
        RulesSet rulesSet = RuleNotation.CoreNotation.INSTANCE.toRulesSet(RuleFormat.JSON, rulesetString);
        rulesSet.withOptions(RuleConfigurationOption.ASYNC_EVALUATION);
        if (pseudoClock || rulesSet.hasAsyncExecution()) {
            rulesSet.withOptions(RuleConfigurationOption.USE_PSEUDO_CLOCK);
        }
        RulesExecutor executor = rulesExecutorContainer.register( RulesExecutorFactory.createRulesExecutor(rulesSet) );
        return executor.getId();
    }

    public void dispose(long sessionId) {
        RulesExecutor rulesExecutor = rulesExecutorContainer.get(sessionId);
        if (rulesExecutor != null) { // ignore if already disposed
            rulesExecutor.dispose();
        }
    }

    public void retractFact(long sessionId, String serializedFact) {
        Map<String, Object> fact = new JSONObject(serializedFact).toMap();
        Map<String, Object> boundFact = Map.of("m", fact);
        rulesExecutorContainer.get(sessionId).processRetract(fact);
    }

    public void assertFact(long sessionId, String serializedFact) {
        Map<String, Object> fact = new JSONObject(serializedFact).toMap();
        rulesExecutorContainer.get(sessionId).processFacts(fact);
    }

    public void assertEvent(long sessionId, String serializedFact) {
        Map<String, Object> fact = new JSONObject(serializedFact).toMap();
        rulesExecutorContainer.get(sessionId).processEvents(fact);
    }

    public String getFacts(long sessionId) {
        RulesExecutor executor = rulesExecutorContainer.get(sessionId);
        if (executor == null) {
            throw new NoSuchElementException("No such session id: " + sessionId + ". " + "Was it disposed?");
        }
        return toJson( executor.getAllFactsAsMap() );
    }

    public void advanceTime(long sessionId, long amount, String unit) {
        rulesExecutorContainer.get(sessionId).advanceTime(amount, TimeUnit.valueOf(unit.toUpperCase()));
    }

    public void shutdown() {
        shutdown = true;
        rulesExecutorContainer.disposeAll();
    }

    private void checkAlive() {
        if (shutdown) {
            throw new IllegalStateException("This AsyncAstRulesEngine is shutting down");
        }
    }
}
