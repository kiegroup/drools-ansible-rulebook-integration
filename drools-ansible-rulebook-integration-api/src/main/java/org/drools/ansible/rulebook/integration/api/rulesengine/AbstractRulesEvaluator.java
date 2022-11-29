package org.drools.ansible.rulebook.integration.api.rulesengine;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
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

    private AutomaticPseudoClock automaticClock;

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
    public void startAutomaticPseudoClock(long period, TimeUnit unit) {
        this.automaticClock = new AutomaticPseudoClock(this, period, unit);
    }

    @Override
    public long getAutomaticPseudoClockPeriod() {
        return this.automaticClock == null ? -1 : this.automaticClock.getPeriod();
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
    public CompletableFuture<List<Match>> fire() {
        return engineEvaluate(() -> getMatches(false));
    }

    @Override
    public CompletableFuture<List<Match>> processFacts(Map<String, Object> factMap) {
        return engineEvaluate(() -> process(factMap, false));
    }

    @Override
    public CompletableFuture<List<Match>> processEvents(Map<String, Object> factMap) {
        return engineEvaluate(() -> process(factMap, true));
    }

    @Override
    public CompletableFuture<List<Match>> advanceTime(long amount, TimeUnit unit ) {
        return engineEvaluate(() -> internalAdvanceTime(amount, unit) );
    }

    @Override
    public CompletableFuture<List<Match>> advanceTimeToMills(long millis) {
        long currentTime = rulesExecutorSession.getPseudoClock().getCurrentTime();
        if (currentTime >= millis) {
            return completeFutureOf(Collections.emptyList());
        }
        return advanceTime(millis - currentTime, TimeUnit.MILLISECONDS);
    }

    @Override
    public CompletableFuture<List<Match>> processRetract(Map<String, Object> json) {
        return engineEvaluate(() -> syncProcessRetract(json));
    }

    protected abstract CompletableFuture<List<Match>> engineEvaluate(Supplier<List<Match>> resultSupplier);

    private List<Match> internalAdvanceTime(long amount, TimeUnit unit) {
        rulesExecutorSession.advanceTime(amount, unit);
        return findMatchedRules();
    }

    protected int internalExecuteFacts(Map<String, Object> factMap) {
        insertFact( factMap, false );
        return rulesExecutorSession.fireAllRules();
    }

    @Override
    public void dispose() {
        if (rulesExecutorContainer != null) {
            rulesExecutorContainer.dispose(getSessionId());
        }
        if (automaticClock != null) {
            automaticClock.shutdown();
        }
        rulesExecutorSession.dispose();
    }

    protected List<Match> process(Map<String, Object> factMap, boolean event) {
        Collection<FactHandle> fhs = insertFacts(factMap, event);
        if (event) {
            fhs.stream()
                    .map(InternalFactHandle.class::cast)
                    .map(InternalFactHandle::getId)
                    .forEach(registerOnlyAgendaFilter::registerephemeralFact);
        }
        return getMatches(event);
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

    protected List<Match> getMatches(boolean event) {
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

    protected List<Match> syncProcessRetract(Map<String, Object> json) {
        List<Match> matches = retractFact(json) ? findMatchedRules() : Collections.emptyList();
        matches = matches.stream().map( MatchDecorator::new ).map(m -> m.withBoundObject("m", json) ).collect(Collectors.toList());
        return matches;
    }

    @Override
    public boolean retractFact(Map<String, Object> factMap) {
        return rulesExecutorSession.deleteFact( mapToFact(factMap, false) );
    }

    protected <T> CompletableFuture<T> completeFutureOf(T value) {
        CompletableFuture<T> future = new CompletableFuture<>();
        future.complete( value );
        return future;
    }
}
