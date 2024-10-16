package org.drools.ansible.rulebook.integration.api.rulesengine;

import org.drools.ansible.rulebook.integration.api.RulesExecutorContainer;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.rule.Match;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public interface RulesEvaluator {

    long getSessionId();

    int rulesCount();

    Collection<?> getAllFacts();

    CompletableFuture<List<Match>> advanceTime(long amount, TimeUnit unit);

    CompletableFuture<Integer> executeFacts(Map<String, Object> factMap);

    CompletableFuture<List<Match>> processFacts(Map<String, Object> factMap);

    CompletableFuture<List<Match>> processEvents(Map<String, Object> factMap);

    CompletableFuture<List<Match>> fire();

    CompletableFuture<List<Match>> processRetractMatchingFacts(Map<String, Object> json, boolean allowPartialMatch, String... keysToExclude);

    void setRulesExecutorContainer(RulesExecutorContainer rulesExecutorContainer);

    void startAutomaticPseudoClock(long period, TimeUnit unit);
    long getAutomaticPseudoClockPeriod();

    SessionStats getSessionStats();

    SessionStats dispose();

    static RulesEvaluator createRulesEvaluator( RulesExecutorSession rulesExecutorSession, boolean async ) {
        return async ? new AsyncRulesEvaluator(rulesExecutorSession) : new SyncRulesEvaluator(rulesExecutorSession);
    }

    KieSession asKieSession();

    void stashFirstEventJsonForValidation(String json);
}
