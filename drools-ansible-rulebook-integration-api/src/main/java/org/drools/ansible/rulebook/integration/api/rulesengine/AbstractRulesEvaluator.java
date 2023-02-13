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
import org.drools.ansible.rulebook.integration.api.io.AstRuleMatch;
import org.drools.ansible.rulebook.integration.api.io.JsonMapper;
import org.drools.ansible.rulebook.integration.api.io.Response;
import org.drools.ansible.rulebook.integration.api.io.RuleExecutorChannel;
import org.drools.core.common.InternalFactHandle;
import org.drools.core.facttemplates.Fact;
import org.kie.api.runtime.rule.Match;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.drools.ansible.rulebook.integration.api.rulesmodel.RulesModelUtil.mapToFact;

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
    public SessionStats dispose() {
        if (rulesExecutorContainer != null) {
            rulesExecutorContainer.removeExecutor(getSessionId());
        }
        if (automaticClock != null) {
            automaticClock.shutdown();
        }
        return rulesExecutorSession.dispose();
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
            return Collections.singletonList( insertFact(factMap, event) );
        }
    }

    private InternalFactHandle insertFact(Map<String, Object> factMap, boolean event) {
        InternalFactHandle fh = (InternalFactHandle) rulesExecutorSession.insert( mapToFact(factMap, event) );
        if (event) {
            rulesExecutorSession.getSessionStats().registerProcessedEvent(fh);
        }
        return fh;
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
        matches = matches.stream().map( FullMatchDecorator::new ).map(m -> m.withBoundObject("m", json) ).collect(Collectors.toList());
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

    protected List<Match> writeResponseOnChannel(List<Match> matches) {
        if (!matches.isEmpty()) { // skip empty result
            byte[] bytes = channel.write(new Response(getSessionId(), AstRuleMatch.asList(matches)));
            rulesExecutorSession.getSessionStats().registerAsyncResponse(bytes);
        }
        return matches;
    }
}
