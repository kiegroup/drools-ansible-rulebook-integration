package org.drools.ansible.rulebook.integration.api.rulesengine;

import org.drools.ansible.rulebook.integration.api.domain.RulesSet;
import org.drools.core.facttemplates.Fact;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.rule.AgendaFilter;
import org.kie.api.runtime.rule.FactHandle;
import org.kie.api.time.SessionPseudoClock;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.BiPredicate;

import static org.drools.ansible.rulebook.integration.api.rulesengine.RegisterOnlyAgendaFilter.SYNTHETIC_RULE_TAG;
import static org.drools.ansible.rulebook.integration.api.rulesmodel.RulesModelUtil.ORIGINAL_MAP_FIELD;


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

    FactHandle insert(Object object) {
        return kieSession.insert(object);
    }

    void delete(FactHandle fh) {
        kieSession.delete(fh);
    }

    int deleteAllMatchingFacts(Map<String, Object> toBeRetracted, boolean allowPartialMatch, String... keysToExclude) {
        BiPredicate<Map<String, Object>, Map<String, Object>> factsComparator = allowPartialMatch ?
                (wmFact, retract) -> wmFact.entrySet().containsAll(retract.entrySet()) :
                (wmFact, retract) -> areFactsEqual(wmFact, retract, keysToExclude);

        Collection<FactHandle> fhs = kieSession.getFactHandles(o -> o instanceof Fact && factsComparator.test( ((Fact) o).asMap(), toBeRetracted ) );
        int result = fhs.size();
        new ArrayList<>( fhs ).forEach( kieSession::delete );
        return result;
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
                if (keyToExclude.equals(wmFactKey)) {
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

    public SessionStatsCollector getSessionStats() {
        return sessionStatsCollector;
    }

    SessionStats dispose() {
        SessionStats stats = sessionStatsCollector.generateStats(this);
        kieSession.dispose();
        return stats;
    }

    long rulesCount() {
        return kieSession.getKieBase().getKiePackages().stream()
                .flatMap(p -> p.getRules().stream())
                .filter( r -> r.getMetaData().get(SYNTHETIC_RULE_TAG) == null )
                .count();
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
