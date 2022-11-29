package org.drools.ansible.rulebook.integration.api.rulesengine;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.drools.ansible.rulebook.integration.api.RulesExecutorContainer;
import org.kie.api.runtime.rule.Match;

public interface RulesEvaluator {

    long getSessionId();

    long rulesCount();

    Collection<?> getAllFacts();

    CompletableFuture<List<Match>> advanceTime(long amount, TimeUnit unit);
    CompletableFuture<List<Match>> advanceTimeToMills(long millis);

    CompletableFuture<Integer> executeFacts(Map<String, Object> factMap);

    CompletableFuture<List<Match>> processFacts(Map<String, Object> factMap);

    CompletableFuture<List<Match>> processEvents(Map<String, Object> factMap);

    CompletableFuture<List<Match>> fire();

    CompletableFuture<List<Match>> processRetract(Map<String, Object> json);

    boolean retractFact(Map<String, Object> factMap);

    void setRulesExecutorContainer(RulesExecutorContainer rulesExecutorContainer);

    void startAutomaticPseudoClock(long period, TimeUnit unit);
    long getAutomaticPseudoClockPeriod();

    void dispose();

    static RulesEvaluator createRulesEvaluator( RulesExecutorSession rulesExecutorSession, boolean async ) {
        return async ? new AsyncRulesEvaluator(rulesExecutorSession) : new SyncRulesEvaluator(rulesExecutorSession);
    }
}
