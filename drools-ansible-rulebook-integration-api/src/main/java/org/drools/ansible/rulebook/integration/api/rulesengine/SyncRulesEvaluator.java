package org.drools.ansible.rulebook.integration.api.rulesengine;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.kie.api.runtime.rule.Match;

public class SyncRulesEvaluator extends AbstractRulesEvaluator {

    public SyncRulesEvaluator(RulesExecutorSession rulesExecutorSession) {
        super(rulesExecutorSession);
    }

    @Override
    public CompletableFuture<List<Match>> fire() {
        return completeFutureOf( getMatches(false) );
    }

    @Override
    public CompletableFuture<Integer> executeFacts(Map<String, Object> factMap) {
        return completeFutureOf( syncExecuteFacts(factMap) );
    }

    @Override
    public CompletableFuture<List<Match>> processFacts(Map<String, Object> factMap) {
        return completeFutureOf( process(factMap, false) );
    }

    @Override
    public CompletableFuture<List<Match>> processEvents(Map<String, Object> factMap) {
        return completeFutureOf( process(factMap, true) );
    }

    @Override
    public CompletableFuture<List<Match>> advanceTime(long amount, TimeUnit unit ) {
        return completeFutureOf( syncAdvanceTime(amount, unit) );
    }

    @Override
    public CompletableFuture<List<Match>> processRetract(Map<String, Object> json) {
        return completeFutureOf( syncProcessRetract(json) );
    }

    private <T> CompletableFuture<T> completeFutureOf(T value) {
        CompletableFuture<T> future = new CompletableFuture<>();
        future.complete( value );
        return future;
    }
}
