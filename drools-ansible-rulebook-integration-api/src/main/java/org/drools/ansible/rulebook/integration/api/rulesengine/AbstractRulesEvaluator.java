package org.drools.ansible.rulebook.integration.api.rulesengine;

import org.drools.ansible.rulebook.integration.api.RulesExecutorContainer;
import org.drools.ansible.rulebook.integration.api.domain.RuleMatch;
import org.drools.ansible.rulebook.integration.api.io.JsonMapper;
import org.drools.ansible.rulebook.integration.api.io.Response;
import org.drools.ansible.rulebook.integration.api.io.RuleExecutorChannel;
import org.drools.core.common.InternalFactHandle;
import org.drools.core.facttemplates.Fact;
import org.kie.api.runtime.rule.Match;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public abstract class AbstractRulesEvaluator implements RulesEvaluator {

    protected static final Logger log = LoggerFactory.getLogger(AbstractRulesEvaluator.class);

    protected final RulesExecutorSession rulesExecutorSession;

    protected final RegisterOnlyAgendaFilter registerOnlyAgendaFilter;

    private RulesExecutorContainer rulesExecutorContainer;

    private AutomaticPseudoClock automaticClock;

    protected RuleExecutorChannel channel;

    protected AsyncExecutor asyncExecutor;

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
        this.asyncExecutor = rulesExecutorContainer.getAsyncExecutor();
        this.channel = rulesExecutorContainer.getChannel();
    }

    @Override
    public void startAutomaticPseudoClock(long period, TimeUnit unit) {
        if (log.isInfoEnabled()) {
            log.info("Start automatic pseudo clock with a tick every " + period + " " + unit.toString().toLowerCase());
        }
        this.automaticClock = new AutomaticPseudoClock(this, period, unit);
    }

    @Override
    public long getAutomaticPseudoClockPeriod() {
        return this.automaticClock == null ? -1 : this.automaticClock.getPeriod();
    }

    @Override
    public int rulesCount() {
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
        return channel != null ?
                engineEvaluate(() -> writeResponseOnChannel(internalAdvanceTime(amount, unit))) :
                engineEvaluate(() -> internalAdvanceTime(amount, unit));
    }

    CompletableFuture<List<Match>> scheduledAdvanceTimeToMills(long millis) {
        long currentTime = rulesExecutorSession.getPseudoClock().getCurrentTime();
        if (currentTime >= millis) {
            return completeFutureOf(Collections.emptyList());
        }

        if (log.isTraceEnabled()) {
            log.trace("Automatic advance of " + (millis - currentTime) + " milliseconds" );
        }

        return asyncExecutor == null ?
                completeFutureOf(internalAdvanceTime(millis - currentTime, TimeUnit.MILLISECONDS)) :
                asyncExecutor.submit(() -> writeResponseOnChannel(internalAdvanceTime(millis - currentTime, TimeUnit.MILLISECONDS) ) );
    }

    @Override
    public CompletableFuture<List<Match>> processRetractMatchingFacts(Map<String, Object> json, boolean allowPartialMatch, String... keysToExclude) {
        return engineEvaluate(() -> retractMatchingFacts(json, allowPartialMatch, keysToExclude));
    }

    private List<Match> retractMatchingFacts(Map<String, Object> json, boolean allowPartialMatch, String[] keysToExclude) {
        List<InternalFactHandle> fhs = rulesExecutorSession.deleteAllMatchingFacts(json, allowPartialMatch, keysToExclude);
        if (fhs.isEmpty()) {
            return Collections.emptyList();
        }
        List<Match> matches = findMatchedRules();
        for (int i = 0; i < matches.size(); i++) {
            Map<String, Object> jsonFact = ((Fact) fhs.get(matches.size() == fhs.size() ? i : 0).getObject()).asMap();
            matches.set(i, enrichMatchWithFact(matches.get(i), jsonFact));
        }
        return matches;
    }

    protected abstract CompletableFuture<List<Match>> engineEvaluate(Supplier<List<Match>> resultSupplier);

    private List<Match> internalAdvanceTime(long amount, TimeUnit unit) {
        rulesExecutorSession.advanceTime(amount, unit);
        return findMatchedRules();
    }

    protected int internalExecuteFacts(Map<String, Object> factMap) {
        rulesExecutorSession.insert( factMap, false );
        return rulesExecutorSession.fireAllRules();
    }

    @Override
    public SessionStats dispose() {
        if (rulesExecutorContainer != null) {
            rulesExecutorContainer.removeExecutor(getSessionId());
        }
        if (automaticClock != null) {
            automaticClock.shutdown();
        }
        return rulesExecutorSession.dispose();
    }

    @Override
    public SessionStats getSessionStats() {
        return rulesExecutorSession.getSessionStats();
    }

    protected List<Match> process(Map<String, Object> factMap, boolean event) {
        Collection<InternalFactHandle> fhs = insertFacts(factMap, event);
        List<Match> matches = getMatches(event);
        if (log.isDebugEnabled()) {
            for (InternalFactHandle fh : fhs) {
                if (fh.isDisconnected()) {
                    String factAsString = fhs.size() == 1 ? JsonMapper.toJson(factMap) : JsonMapper.toJson(((Fact)fh.getObject()).asMap());
                    log.debug((event ? "Event " : "Fact ") + factAsString + " didn't match any rule and has been immediately discarded");
                }
            }
        }
        return matches;
    }

    private Collection<InternalFactHandle> insertFacts(Map<String, Object> factMap, boolean event) {
        String key = event ? "events" : "facts";
        if (factMap.size() == 1 && factMap.containsKey(key)) {
            return ((List<Map<String, Object>>)factMap.get(key)).stream()
                    .flatMap(map -> this.insertFacts(map, event).stream())
                    .collect(Collectors.toList());
        } else {
            return Collections.singletonList( rulesExecutorSession.insert(factMap, event) );
        }
    }

    protected List<Match> getMatches(boolean event) {
        List<Match> matches = findMatchedRules();
        return !event || matches.size() < 2 ?
                matches :
                // when processing an event return only the matches for the first matched rule
                matches.stream().takeWhile(match -> match.getRule().getName().equals(matches.get(0).getRule().getName())).collect(Collectors.toList());
    }

    private synchronized List<Match> findMatchedRules() {
        rulesExecutorSession.setExecuteActions(false);
        rulesExecutorSession.fireAllRules(registerOnlyAgendaFilter);
        rulesExecutorSession.setExecuteActions(true);
        return registerOnlyAgendaFilter.finalizeAndGetResults();
    }

    private Match enrichMatchWithFact(Match match, Map<String, Object> jsonFact) {
        return new FullMatchDecorator(match).withBoundObject("m", jsonFact);
    }

    protected <T> CompletableFuture<T> completeFutureOf(T value) {
        CompletableFuture<T> future = new CompletableFuture<>();
        future.complete( value );
        return future;
    }

    protected List<Match> writeResponseOnChannel(List<Match> matches) {
        if (!matches.isEmpty()) { // skip empty result
            byte[] bytes = channel.write(new Response(getSessionId(), RuleMatch.asList(matches)));
            rulesExecutorSession.registerAsyncResponse(bytes);
        }
        return matches;
    }
}
