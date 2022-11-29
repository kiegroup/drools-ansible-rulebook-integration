package org.drools.ansible.rulebook.integration.api.rulesengine;

import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.drools.core.facttemplates.Fact;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.rule.AgendaFilter;
import org.kie.api.runtime.rule.FactHandle;
import org.kie.api.time.SessionPseudoClock;


public class RulesExecutorSession {

    private final KieSession kieSession;

    private final RulesExecutionController rulesExecutionController;

    private final long id;

    public RulesExecutorSession(KieSession kieSession, RulesExecutionController rulesExecutionController, long id) {
        this.kieSession = kieSession;
        this.rulesExecutionController = rulesExecutionController;
        this.id = id;
    }

    long getId() {
        return id;
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

    boolean deleteFact(Fact toBeRetracted) {
        return kieSession.getFactHandles(o -> o instanceof Fact && Objects.equals(((Fact) o).asMap(), toBeRetracted.asMap()))
                .stream().findFirst()
                .map( fh -> {
                    kieSession.delete( fh );
                    return true;
                }).orElse(false);
    }

    int fireAllRules() {
        return kieSession.fireAllRules();
    }

    int fireAllRules(AgendaFilter agendaFilter) {
        return kieSession.fireAllRules(agendaFilter);
    }

    void dispose() {
        kieSession.dispose();
    }

    long rulesCount() {
        return kieSession.getKieBase().getKiePackages().stream().mapToLong(p -> p.getRules().size()).sum();
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
