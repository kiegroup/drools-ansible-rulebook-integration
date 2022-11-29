package org.drools.ansible.rulebook.integration.api.rulesengine;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import org.kie.api.runtime.rule.Match;

public class AsyncRulesEvaluator extends AbstractRulesEvaluator {

    public AsyncRulesEvaluator(RulesExecutorSession rulesExecutorSession) {
        super(rulesExecutorSession);
    }

    @Override
    public CompletableFuture<Integer> executeFacts(Map<String, Object> factMap) {
        return asyncExecutor.submit( () -> internalExecuteFacts(factMap) );
    }

    @Override
    protected CompletableFuture<List<Match>> engineEvaluate(Supplier<List<Match>> resultSupplier) {
        return asyncExecutor.submit(() -> writeResponseOnChannel(resultSupplier.get()));
    }
}
