package org.drools.ansible.rulebook.integration.core.jpy;

import java.util.List;
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
import org.drools.ansible.rulebook.integration.api.io.AstRuleMatch;
import org.json.JSONObject;
import org.kie.api.runtime.rule.Match;

import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.toJson;

public class AstRulesEngine {

    private final RulesExecutorContainer rulesExecutorContainer = new RulesExecutorContainer();

    private boolean shutdown = false;

    public long createRuleset(String rulesetString) {
        return createRulesetWithOptions(rulesetString, true);
    }

    public long createRulesetWithOptions(String rulesetString, boolean async) {
        checkAlive();
        RulesSet rulesSet = RuleNotation.CoreNotation.INSTANCE.toRulesSet(RuleFormat.JSON, rulesetString);
//        boolean async = rulesSet.hasAsyncExecution();
        if (async) {
            rulesSet.withOptions(RuleConfigurationOption.USE_PSEUDO_CLOCK);
            rulesExecutorContainer.allowAsync();
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

    /**
     * @return error code (currently always 0)
     */
    public String retractFact(long sessionId, String serializedFact) {
        Map<String, Object> fact = new JSONObject(serializedFact).toMap();
        List<Match> matches = rulesExecutorContainer.get(sessionId).processRetract(fact).join();
        return toJson( AstRuleMatch.asList(matches) );
    }

    public String assertFact(long sessionId, String serializedFact) {
        Map<String, Object> fact = new JSONObject(serializedFact).toMap();
        List<Match> matches = rulesExecutorContainer.get(sessionId).processFacts(fact).join();
        return toJson(AstRuleMatch.asList(matches));
    }

    public String assertEvent(long sessionId, String serializedFact) {
        Map<String, Object> fact = new JSONObject(serializedFact).toMap();
        List<Match> matches = rulesExecutorContainer.get(sessionId).processEvents(fact).join();
        return toJson(AstRuleMatch.asList(matches));
    }

    public String getFacts(long sessionId) {
        RulesExecutor executor = rulesExecutorContainer.get(sessionId);
        if (executor == null) {
            throw new NoSuchElementException("No such session id: " + sessionId + ". " + "Was it disposed?");
        }
        return toJson(executor.getAllFactsAsMap());
    }

    /**
     * Advances the clock time in the specified unit amount.
     *
     * @param amount the amount of units to advance in the clock
     * @param unit the used time unit
     * @return the events that fired
     */
    public String advanceTime(long sessionId, long amount, String unit) {
        List<Match> matches = rulesExecutorContainer.get(sessionId).advanceTime(amount, TimeUnit.valueOf(unit.toUpperCase())).join();
        return toJson(AstRuleMatch.asList(matches));
    }

    public void shutdown() {
        shutdown = true;
        rulesExecutorContainer.disposeAll();
    }

    public int port() {
        return rulesExecutorContainer.port();
    }

    private void checkAlive() {
        if (shutdown) {
            throw new IllegalStateException("This AstRulesEngine is shutting down");
        }
    }
}
