package org.drools.ansible.rulebook.integration.api;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.drools.ansible.rulebook.integration.api.rulesengine.RulesEvaluator;
import org.drools.ansible.rulebook.integration.api.rulesengine.RulesExecutorSession;
import org.drools.ansible.rulebook.integration.api.rulesengine.SessionStats;
import org.drools.core.facttemplates.Fact;
import org.json.JSONObject;
import org.kie.api.runtime.rule.Match;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.toJson;

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

    public SessionStats dispose() {
        if (log.isInfoEnabled()) {
            log.info("Disposing session with id: " + getId());
        }
        return rulesEvaluator.dispose();
    }

    public long rulesCount() {
        return rulesEvaluator.rulesCount();
    }

    public CompletableFuture<Integer> executeFacts(String json) {
        return executeFacts( new JSONObject(json).toMap() );
    }

    public CompletableFuture<Integer> executeFacts(Map<String, Object> factMap) {
        return rulesEvaluator.executeFacts(factMap);
    }

    public CompletableFuture<List<Match>> processFacts(String json) {
        return processFacts( new JSONObject(json).toMap() );
    }

    public CompletableFuture<List<Match>> processFacts(Map<String, Object> factMap) {
        return rulesEvaluator.processFacts(factMap);
    }

    public CompletableFuture<List<Match>> processEvents(String json) {
        return processEvents( new JSONObject(json).toMap() );
    }

    public CompletableFuture<List<Match>> processEvents(Map<String, Object> factMap) {
        return rulesEvaluator.processEvents(factMap);
    }

    public CompletableFuture<List<Match>> fire() {
        return rulesEvaluator.fire();
    }

    public CompletableFuture<List<Match>> processRetract(String json) {
        return processRetract(new JSONObject(json).toMap());
    }

    public CompletableFuture<List<Match>> processRetract(Map<String, Object> json) {
        return rulesEvaluator.processRetract(json);
    }

    public CompletableFuture<List<Match>> processRetractMatchingFacts(String json) {
        return processRetractMatchingFacts(new JSONObject(json).toMap());
    }

    public CompletableFuture<List<Match>> processRetractMatchingFacts(Map<String, Object> json) {
        return rulesEvaluator.processRetractMatchingFacts(json);
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
