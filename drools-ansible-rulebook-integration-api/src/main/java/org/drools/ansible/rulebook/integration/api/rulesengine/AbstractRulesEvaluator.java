package org.drools.ansible.rulebook.integration.api.rulesengine;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.drools.ansible.rulebook.integration.api.RulesExecutorContainer;
import org.drools.core.common.InternalFactHandle;
import org.kie.api.runtime.rule.FactHandle;
import org.kie.api.runtime.rule.Match;

import static org.drools.ansible.rulebook.integration.api.rulesmodel.RulesModelUtil.mapToFact;

public abstract class AbstractRulesEvaluator implements RulesEvaluator {

    protected final RulesExecutorSession rulesExecutorSession;

    protected final RegisterOnlyAgendaFilter registerOnlyAgendaFilter;

    private RulesExecutorContainer rulesExecutorContainer;

    public AbstractRulesEvaluator(RulesExecutorSession rulesExecutorSession) {
        this.rulesExecutorSession = rulesExecutorSession;
        this.registerOnlyAgendaFilter = new RegisterOnlyAgendaFilter(rulesExecutorSession);
    }

    @Override
    public long getSessionId() {
        return rulesExecutorSession.getId();
    }

    @Override
    public void setRulesExecutorContainer(RulesExecutorContainer rulesExecutorContainer) {
        this.rulesExecutorContainer = rulesExecutorContainer;
    }

    @Override
    public long rulesCount() {
        return rulesExecutorSession.rulesCount();
    }

    @Override
    public Collection<?> getAllFacts() {
        return rulesExecutorSession.getObjects();
    }

    @Override
    public List<Match> advanceTime(long amount, TimeUnit unit ) {
        rulesExecutorSession.advanceTime(amount, unit);
        return writeResponse( findMatchedRules() );
    }

    @Override
    public int executeFacts(Map<String, Object> factMap) {
        insertFact( factMap, false );
        return rulesExecutorSession.fireAllRules();
    }

    @Override
    public List<Match> processFacts(Map<String, Object> factMap) {
        return process(factMap, false);
    }

    @Override
    public List<Match> processEvents(Map<String, Object> factMap) {
        return process(factMap, true);
    }

    @Override
    public List<Match> fire() {
        return getMatches(false);
    }

    @Override
    public List<Match> processRetract(Map<String, Object> json) {
        List<Match> matches = retractFact(json) ? findMatchedRules() : Collections.emptyList();
        matches = matches.stream().map( MatchDecorator::new ).map(m -> m.withBoundObject("m", json) ).collect(Collectors.toList());
        return writeResponse( matches );
    }

    @Override
    public boolean retractFact(Map<String, Object> factMap) {
        return rulesExecutorSession.deleteFact( mapToFact(factMap, false) );
    }

    @Override
    public void dispose() {
        if (rulesExecutorContainer != null) {
            rulesExecutorContainer.dispose(getSessionId());
        }
        rulesExecutorSession.dispose();
    }

    protected abstract List<Match> writeResponse(List<Match> matches);

    private List<Match> process(Map<String, Object> factMap, boolean event) {
        Collection<FactHandle> fhs = insertFacts(factMap, event);
        if (event) {
            fhs.stream()
                    .map(InternalFactHandle.class::cast)
                    .map(InternalFactHandle::getId)
                    .forEach(registerOnlyAgendaFilter::registerephemeralFact);
        }
        return writeResponse( getMatches(event) );
    }

    private Collection<FactHandle> insertFacts(Map<String, Object> factMap, boolean event) {
        String key = event ? "events" : "facts";
        if (factMap.size() == 1 && factMap.containsKey(key)) {
            return ((List<Map<String, Object>>)factMap.get(key)).stream()
                    .flatMap(map -> this.insertFacts(map, event).stream())
                    .collect(Collectors.toList());
        } else {
            return Collections.singletonList( insertFact(factMap, event) );
        }
    }

    private FactHandle insertFact(Map<String, Object> factMap, boolean event) {
        return rulesExecutorSession.insert( mapToFact(factMap, event) );
    }

    private List<Match> getMatches(boolean event) {
        List<Match> matches = findMatchedRules();
        return !event || matches.size() < 2 ?
                matches :
                // when processing an event return only the matches for the first matched rule
                matches.stream().takeWhile(match -> match.getRule().getName().equals(matches.get(0).getRule().getName())).collect(Collectors.toList());
    }

    private List<Match> findMatchedRules() {
        rulesExecutorSession.setExecuteActions(false);
        rulesExecutorSession.fireAllRules(registerOnlyAgendaFilter);
        rulesExecutorSession.setExecuteActions(true);
        return registerOnlyAgendaFilter.finalizeAndGetResults();
    }
}
