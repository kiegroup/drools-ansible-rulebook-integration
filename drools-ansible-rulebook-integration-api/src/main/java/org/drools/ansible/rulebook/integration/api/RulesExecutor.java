package org.drools.ansible.rulebook.integration.api;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.drools.ansible.rulebook.integration.api.rulesengine.RulesEvaluator;
import org.drools.ansible.rulebook.integration.api.rulesengine.RulesExecutorSession;
import org.drools.core.facttemplates.Fact;
import org.json.JSONObject;
import org.kie.api.runtime.rule.Match;

import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.toJson;

public class RulesExecutor {

    private final RulesEvaluator rulesEvaluator;

    RulesExecutor(RulesExecutorSession rulesExecutorSession, boolean async) {
        this.rulesEvaluator = RulesEvaluator.createRulesEvaluator(rulesExecutorSession, async);
    }

    public void setRulesExecutorContainer(RulesExecutorContainer rulesExecutorContainer) {
        rulesEvaluator.setRulesExecutorContainer(rulesExecutorContainer);
    }

    public long getId() {
        return rulesEvaluator.getSessionId();
    }

    public void dispose() {
        rulesEvaluator.dispose();
    }

    public long rulesCount() {
        return rulesEvaluator.rulesCount();
    }

    public int executeFacts(String json) {
        return executeFacts( new JSONObject(json).toMap() );
    }

    public int executeFacts(Map<String, Object> factMap) {
        return rulesEvaluator.executeFacts(factMap);
    }

    public List<Match> processFacts(String json) {
        return processFacts( new JSONObject(json).toMap() );
    }

    public List<Match> processFacts(Map<String, Object> factMap) {
        return rulesEvaluator.processFacts(factMap);
    }

    public List<Match> processEvents(String json) {
        return processEvents( new JSONObject(json).toMap() );
    }

    public List<Match> processEvents(Map<String, Object> factMap) {
        return rulesEvaluator.processEvents(factMap);
    }

    public List<Match> fire() {
        return rulesEvaluator.fire();
    }

    public List<Match> processRetract(String json) {
        return processRetract(new JSONObject(json).toMap());
    }

    public List<Match> processRetract(Map<String, Object> json) {
        return rulesEvaluator.processRetract(json);
    }

    public boolean retractFact(Map<String, Object> factMap) {
        return rulesEvaluator.retractFact( factMap );
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

    public List<Match> advanceTime(long amount, TimeUnit unit ) {
        return rulesEvaluator.advanceTime(amount, unit );
    }
}
