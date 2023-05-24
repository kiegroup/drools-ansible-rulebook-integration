package org.drools.ansible.rulebook.integration.api.rulesengine;

import org.drools.ansible.rulebook.integration.api.domain.RulesSet;
import org.drools.core.common.InternalFactHandle;
import org.drools.core.facttemplates.Event;
import org.drools.core.facttemplates.Fact;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.rule.AgendaFilter;
import org.kie.api.runtime.rule.FactHandle;
import org.kie.api.runtime.rule.Match;
import org.kie.api.time.SessionPseudoClock;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;

import static org.drools.ansible.rulebook.integration.api.rulesmodel.RulesModelUtil.ORIGINAL_MAP_FIELD;
import static org.drools.ansible.rulebook.integration.api.rulesmodel.RulesModelUtil.mapToFact;


public class RulesExecutorSession {

    private final RulesSet rulesSet;

    private final KieSession kieSession;

    private final RulesExecutionController rulesExecutionController;

    private final long id;

    private final SessionStatsCollector sessionStatsCollector;

    public RulesExecutorSession(RulesSet rulesSet, KieSession kieSession, RulesExecutionController rulesExecutionController, long id) {
        this.rulesSet = rulesSet;
        this.kieSession = kieSession;
        this.rulesExecutionController = rulesExecutionController;
        this.id = id;
        this.sessionStatsCollector = new SessionStatsCollector(id);

        initClock();
    }

    private void initClock() {
        getPseudoClock().advanceTime(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    }

    long getId() {
        return id;
    }

    String getRuleSetName() {
        return rulesSet.getName();
    }

    Collection<? extends Object> getObjects() {
        return kieSession.getObjects();
    }

    InternalFactHandle insert(Map<String, Object> factMap, boolean event) {
        Fact fact = mapToFact(factMap, event);
        if (event) {
            ((Event) fact).withExpiration(rulesSet.getEventsTtl().getAmount(), rulesSet.getEventsTtl().getTimeUnit());
        }
        InternalFactHandle fh = (InternalFactHandle) kieSession.insert(fact);
        if (event) {
            sessionStatsCollector.registerProcessedEvent(fh);
        }
        return fh;
    }

    void delete(FactHandle fh) {
        kieSession.delete(fh);
    }

    List<InternalFactHandle> deleteAllMatchingFacts(Map<String, Object> toBeRetracted, boolean allowPartialMatch, String... keysToExclude) {
        BiPredicate<Map<String, Object>, Map<String, Object>> factsComparator = allowPartialMatch ?
                (wmFact, retract) -> wmFact.entrySet().containsAll(retract.entrySet()) :
                (wmFact, retract) -> areFactsEqual(wmFact, retract, keysToExclude);

        Collection<FactHandle> fhs = kieSession.getFactHandles(o -> o instanceof Fact && factsComparator.test( ((Fact) o).asMap(), toBeRetracted ) );
        return new ArrayList<>( fhs ).stream().peek( kieSession::delete ).map( InternalFactHandle.class::cast ).collect(Collectors.toList());
    }

    private static boolean areFactsEqual(Map<String, Object> wmFact, Map<String, Object> retract, String... keysToExclude) {
        Set<String> checkedKeys = new HashSet<>();
        for (Map.Entry<String, Object> wmFactValue : wmFact.entrySet()) {
            String wmFactKey = wmFactValue.getKey();
            if (!isKeyToBeIgnored(wmFactKey, keysToExclude) && !wmFactValue.getValue().equals(retract.get(wmFactKey))) {
                return false;
            }
            checkedKeys.add(wmFactKey);
        }
        return checkedKeys.size() == wmFact.size();
    }

    private static boolean isKeyToBeIgnored(String wmFactKey, String... keysToExclude) {
        if (keysToExclude != null && keysToExclude.length > 0) {
            for (String keyToExclude : keysToExclude) {
                if (wmFactKey.equals(keyToExclude) || wmFactKey.startsWith(keyToExclude + ".")) {
                    return true;
                }
            }
        }
        return wmFactKey.equals(ORIGINAL_MAP_FIELD);
    }

    int fireAllRules() {
        return kieSession.fireAllRules();
    }

    int fireAllRules(AgendaFilter agendaFilter) {
        return kieSession.fireAllRules(agendaFilter);
    }

    SessionStats dispose() {
        SessionStats stats = getSessionStats();
        kieSession.dispose();
        return stats;
    }

    SessionStats getSessionStats() {
        return sessionStatsCollector.generateStats(this);
    }

    public void registerMatch(Match match) {
        sessionStatsCollector.registerMatch(this, match);
    }

    public void registerMatchedEvents(Collection<FactHandle> events) {
        sessionStatsCollector.registerMatchedEvents(events);
    }

    public void registerAsyncResponse(byte[] bytes) {
        sessionStatsCollector.registerAsyncResponse(bytes);
    }

    int rulesCount() {
        return rulesSet.getEnabledRulesNumber();
    }

    int disabledRulesCount() {
        return rulesSet.getDisabledRulesNumber();
    }

    void advanceTime( long amount, TimeUnit unit ) {
        SessionPseudoClock clock = getPseudoClock();
        clock.advanceTime(amount, unit);
    }

    SessionPseudoClock getPseudoClock() {
        return kieSession.getSessionClock();
    }

    void setExecuteActions(boolean executeActions) {
        rulesExecutionController.setExecuteActions(executeActions);
    }
}
