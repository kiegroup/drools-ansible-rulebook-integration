package org.drools.ansible.rulebook.integration.api.rulesengine;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.drools.ansible.rulebook.integration.api.domain.RulesSet;
import org.drools.core.facttemplates.Fact;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.rule.AgendaFilter;
import org.kie.api.runtime.rule.FactHandle;
import org.kie.api.time.SessionPseudoClock;

import static org.drools.ansible.rulebook.integration.api.rulesengine.RegisterOnlyAgendaFilter.SYNTHETIC_RULE_TAG;


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

    boolean deleteFact(Map<String, Object> toBeRetracted) {
        return kieSession.getFactHandles(o -> o instanceof Fact && ((Fact) o).asMap().equals( toBeRetracted ))
                .stream().findFirst()
                .map( fh -> {
                    kieSession.delete( fh );
                    return true;
                }).orElse(false);
    }

    int deleteAllMatchingFacts(Map<String, Object> toBeRetracted) {
        Set<Map.Entry<String, Object>> retractingEntrySet = toBeRetracted.entrySet();
        Collection<FactHandle> fhs = kieSession.getFactHandles(o -> o instanceof Fact && ((Fact) o).asMap().entrySet().containsAll( retractingEntrySet ) );
        int result = fhs.size();
        new ArrayList<>( fhs ).forEach( kieSession::delete );
        return result;
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
