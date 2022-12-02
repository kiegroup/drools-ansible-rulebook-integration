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
        // if there is a running automatic clock all engine evaluations have to be enqueued on the single threaded async executor to avoid race condition
        return hasAsyncChannel() ?
                completeFutureOf( asyncExecutor.submit(() -> resultSupplier.get()).join() ) :
                completeFutureOf( resultSupplier.get() );
    }

    private boolean hasAsyncChannel() {
        boolean hasAsyncChannel = asyncExecutor != null;
        if (hasAsyncChannel && !channel.isConnected()) {
            throw new IllegalStateException("No connected client on server socket");
        }
        return hasAsyncChannel;
    }
}
