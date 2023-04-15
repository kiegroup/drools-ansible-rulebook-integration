package org.drools.ansible.rulebook.integration.core.jpy;

import org.drools.ansible.rulebook.integration.api.*;
import org.drools.ansible.rulebook.integration.api.domain.RulesSet;

import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

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

    public String dispose(long sessionId) {
        RulesExecutor rulesExecutor = rulesExecutorContainer.get(sessionId);
        return rulesExecutor == null ? null : toJson( rulesExecutor.dispose() );
    }

    public String sessionStats(long sessionId) {
        RulesExecutor rulesExecutor = rulesExecutorContainer.get(sessionId);
        return rulesExecutor == null ? null : toJson( rulesExecutor.getSessionStats() );
    }

    @Deprecated
    public void retractFact(long sessionId, String serializedFact) {
        rulesExecutorContainer.get(sessionId).processRetractMatchingFacts(serializedFact, false);
    }

    public void retractMatchingFacts(long sessionId, String serializedFact, boolean allowPartialMatch, String... keysToExclude) {
        rulesExecutorContainer.get(sessionId).processRetractMatchingFacts(serializedFact, allowPartialMatch, keysToExclude);
    }

    public void assertFact(long sessionId, String serializedFact) {
        rulesExecutorContainer.get(sessionId).processFacts(serializedFact);
    }

    public void assertEvent(long sessionId, String serializedFact) {
        rulesExecutorContainer.get(sessionId).processEvents(serializedFact);
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
