package org.drools.ansible.rulebook.integration.api;

import org.drools.ansible.rulebook.integration.api.rulesengine.RulesEvaluator;
import org.drools.ansible.rulebook.integration.api.rulesengine.RulesExecutorSession;
import org.drools.ansible.rulebook.integration.api.rulesengine.SessionStats;
import org.drools.base.facttemplates.Fact;
import org.kie.api.runtime.rule.Match;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.toJson;
import static org.drools.ansible.rulebook.integration.api.rulesmodel.RulesModelUtil.asFactMap;

public class RulesExecutor {

    protected static final Logger log = LoggerFactory.getLogger(RulesExecutor.class);

    private final RulesEvaluator rulesEvaluator;

    RulesExecutor(RulesExecutorSession rulesExecutorSession, boolean async) {
        this.rulesEvaluator = RulesEvaluator.createRulesEvaluator(rulesExecutorSession, async);
    }

    void startAutomaticPseudoClock(long period, TimeUnit unit) {
        this.rulesEvaluator.startAutomaticPseudoClock(period, unit);
    }

    public long getAutomaticPseudoClockPeriod() {
        return this.rulesEvaluator.getAutomaticPseudoClockPeriod();
    }

    public void setRulesExecutorContainer(RulesExecutorContainer rulesExecutorContainer) {
        rulesEvaluator.setRulesExecutorContainer(rulesExecutorContainer);
    }

    public long getId() {
        return rulesEvaluator.getSessionId();
    }

    public SessionStats getSessionStats() {
        return rulesEvaluator.getSessionStats();

    }
    public SessionStats dispose() {
        SessionStats sessionStats = rulesEvaluator.dispose();
        if (log.isInfoEnabled()) {
            log.info("Disposing session with id: " + getId() + "; " + sessionStats);
        }
        return sessionStats;
    }

    public long rulesCount() {
        return rulesEvaluator.rulesCount();
    }

    public CompletableFuture<Integer> executeFacts(String json) {
        return rulesEvaluator.executeFacts(asFactMap(json));
    }

    public CompletableFuture<List<Match>> processFacts(String json) {
        return rulesEvaluator.processFacts(asFactMap(json));
    }

    public CompletableFuture<List<Match>> processEvents(String json) {
        return rulesEvaluator.processEvents(asFactMap(json));
    }

    public CompletableFuture<List<Match>> fire() {
        return rulesEvaluator.fire();
    }

    public CompletableFuture<List<Match>> processRetractMatchingFacts(String json, boolean allowPartialMatch, String... keysToExclude) {
        return rulesEvaluator.processRetractMatchingFacts(asFactMap(json), allowPartialMatch, keysToExclude);
    }

    public Collection<?> getAllFacts() {
        return rulesEvaluator.getAllFacts();
    }

    public List<Map<String, Object>> getAllFactsAsMap() {
        return getAllFacts().stream().map(Fact.class::cast).map(Fact::asMap).collect(Collectors.toList());
    }

    public String getAllFactsAsJson() {
        return toJson(getAllFactsAsMap());
    }

    public CompletableFuture<List<Match>> advanceTime(long amount, TimeUnit unit ) {
        return rulesEvaluator.advanceTime(amount, unit );
    }
}
