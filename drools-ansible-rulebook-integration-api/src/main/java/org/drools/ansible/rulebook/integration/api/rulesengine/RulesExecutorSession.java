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

    public long getId() {
        return id;
    }

    public Collection<? extends Object> getObjects() {
        return kieSession.getObjects();
    }

    public FactHandle insert(Object object) {
        return kieSession.insert(object);
    }

    public void delete(FactHandle fh) {
        kieSession.delete(fh);
    }

    public boolean deleteFact(Fact toBeRetracted) {
        return kieSession.getFactHandles(o -> o instanceof Fact && Objects.equals(((Fact) o).asMap(), toBeRetracted.asMap()))
                .stream().findFirst()
                .map( fh -> {
                    kieSession.delete( fh );
                    return true;
                }).orElse(false);
    }

    public int fireAllRules() {
        return kieSession.fireAllRules();
    }

    public int fireAllRules(AgendaFilter agendaFilter) {
        return kieSession.fireAllRules(agendaFilter);
    }

    public void dispose() {
        kieSession.dispose();
    }

    public long rulesCount() {
        return kieSession.getKieBase().getKiePackages().stream().mapToLong(p -> p.getRules().size()).sum();
    }

    public void advanceTime( long amount, TimeUnit unit ) {
        SessionPseudoClock clock = kieSession.getSessionClock();
        clock.advanceTime(amount, unit);
    }

    public boolean isExecuteActions() {
        return rulesExecutionController.executeActions();
    }

    public void setExecuteActions(boolean executeActions) {
        rulesExecutionController.setExecuteActions(executeActions);
    }
}
