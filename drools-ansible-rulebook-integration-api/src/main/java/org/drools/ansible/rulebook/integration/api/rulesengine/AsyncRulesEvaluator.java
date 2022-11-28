package org.drools.ansible.rulebook.integration.api.rulesengine;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

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

    public long getSessionId() {
        return rulesExecutorSession.getId();
    }

    @Override
    public void setRulesExecutorContainer(RulesExecutorContainer rulesExecutorContainer) {
        super.setRulesExecutorContainer(rulesExecutorContainer);
        this.asyncExecutor = rulesExecutorContainer.getAsyncExecutor();
        this.channel = rulesExecutorContainer.getChannel();
    }

    @Override
    public CompletableFuture<List<Match>> fire() {
        return asyncExecutor.submit( () -> writeResponseOnChannel( getMatches(false) ) );
    }

    @Override
    public CompletableFuture<Integer> executeFacts(Map<String, Object> factMap) {
        return asyncExecutor.submit( () -> syncExecuteFacts(factMap) );
    }

    @Override
    public CompletableFuture<List<Match>> processFacts(Map<String, Object> factMap) {
        return asyncExecutor.submit( () -> writeResponseOnChannel( process(factMap, false) ) );
    }

    @Override
    public CompletableFuture<List<Match>> processEvents(Map<String, Object> factMap) {
        return asyncExecutor.submit( () -> writeResponseOnChannel( process(factMap, true) ) );
    }

    @Override
    public CompletableFuture<List<Match>> advanceTime(long amount, TimeUnit unit ) {
        return asyncExecutor.submit( () -> writeResponseOnChannel( syncAdvanceTime(amount, unit) ) );
    }

    @Override
    public CompletableFuture<List<Match>> processRetract(Map<String, Object> json) {
        return asyncExecutor.submit( () -> writeResponseOnChannel( syncProcessRetract(json) ) );
    }

    protected List<Match> writeResponseOnChannel(List<Match> matches) {
        if (!matches.isEmpty()) { // skip empty result
            channel.write(new Response(getSessionId(), AstRuleMatch.asList(matches)));
        }
        return matches;
    }
}
