package org.drools.ansible.rulebook.integration.api;

import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.drools.ansible.rulebook.integration.api.rulesmodel.PrototypeFactory;
import org.drools.core.facttemplates.Fact;
import org.drools.model.Prototype;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.rule.AgendaFilter;
import org.kie.api.runtime.rule.FactHandle;
import org.kie.api.time.SessionPseudoClock;

class RulesExecutorSession {

    private final PrototypeFactory prototypeFactory;

    private final KieSession kieSession;

    private final RulesExecutorHolder rulesExecutorHolder;

    public RulesExecutorSession(PrototypeFactory prototypeFactory, KieSession kieSession, RulesExecutorHolder rulesExecutorHolder) {
        this.prototypeFactory = prototypeFactory;
        this.kieSession = kieSession;
        this.rulesExecutorHolder = rulesExecutorHolder;
    }

    public void setRulesExecutor(RulesExecutor rulesExecutor) {
        rulesExecutorHolder.set(rulesExecutor);
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

    public Prototype getPrototype() {
        return prototypeFactory.getPrototype();
    }

    public void advanceTime( long amount, TimeUnit unit ) {
        SessionPseudoClock clock = kieSession.getSessionClock();
        clock.advanceTime(amount, unit);
    }

    static class RulesExecutorHolder implements Supplier<RulesExecutor> {
        private RulesExecutor rulesExecutor;

        @Override
        public RulesExecutor get() {
            return rulesExecutor;
        }

        public void set(RulesExecutor rulesExecutor) {
            this.rulesExecutor = rulesExecutor;
        }
    }
}
