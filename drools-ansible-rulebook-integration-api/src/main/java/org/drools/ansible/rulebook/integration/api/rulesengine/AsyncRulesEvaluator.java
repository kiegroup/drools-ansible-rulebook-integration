package org.drools.ansible.rulebook.integration.api.rulesengine;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import org.drools.ansible.rulebook.integration.api.RulesExecutorContainer;
import org.drools.ansible.rulebook.integration.api.io.AstRuleMatch;
import org.drools.ansible.rulebook.integration.api.io.Response;
import org.drools.ansible.rulebook.integration.api.io.RuleExecutorChannel;
import org.kie.api.runtime.rule.Match;

public class AsyncRulesEvaluator extends AbstractRulesEvaluator {

    private RuleExecutorChannel channel;

    private AsyncExecutor asyncExecutor;

    public AsyncRulesEvaluator(RulesExecutorSession rulesExecutorSession) {
        super(rulesExecutorSession);
    }

    @Override
    public void setRulesExecutorContainer(RulesExecutorContainer rulesExecutorContainer) {
        super.setRulesExecutorContainer(rulesExecutorContainer);
        this.asyncExecutor = rulesExecutorContainer.getAsyncExecutor();
        this.channel = rulesExecutorContainer.getChannel();
    }

    @Override
    public CompletableFuture<Integer> executeFacts(Map<String, Object> factMap) {
        return asyncExecutor.submit( () -> syncExecuteFacts(factMap) );
    }

    @Override
    protected CompletableFuture<List<Match>> engineEvaluate(Supplier<List<Match>> resultSupplier) {
        return asyncExecutor.submit(() -> writeResponseOnChannel(resultSupplier.get()));
    }

    private List<Match> writeResponseOnChannel(List<Match> matches) {
        if (!matches.isEmpty()) { // skip empty result
            channel.write(new Response(getSessionId(), AstRuleMatch.asList(matches)));
        }
        return matches;
    }
}
