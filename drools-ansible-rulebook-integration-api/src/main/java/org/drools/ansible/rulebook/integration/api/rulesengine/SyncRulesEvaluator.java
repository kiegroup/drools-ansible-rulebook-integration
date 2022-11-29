package org.drools.ansible.rulebook.integration.api.rulesengine;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import org.kie.api.runtime.rule.Match;

public class SyncRulesEvaluator extends AbstractRulesEvaluator {

    public SyncRulesEvaluator(RulesExecutorSession rulesExecutorSession) {
        super(rulesExecutorSession);
    }

    @Override
    public CompletableFuture<Integer> executeFacts(Map<String, Object> factMap) {
        return completeFutureOf( internalExecuteFacts(factMap) );
    }

    @Override
    protected CompletableFuture<List<Match>> engineEvaluate(Supplier<List<Match>> resultSupplier) {
        return completeFutureOf(resultSupplier.get());
    }
}
