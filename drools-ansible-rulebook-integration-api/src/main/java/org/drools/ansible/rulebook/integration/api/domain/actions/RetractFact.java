package org.drools.ansible.rulebook.integration.api.domain.actions;

import org.drools.model.Drools;
import org.drools.ansible.rulebook.integration.api.RulesExecutor;

public class RetractFact extends FactAction {

    static final String ACTION_NAME = "retract_fact";

    @Override
    public String toString() {
        return "RetractFact{" +
                "ruleset='" + getRuleset() + '\'' +
                ", fact=" + getFact() +
                '}';
    }

    @Override
    public void execute(Drools drools) {
        System.out.println("Retracting " + getFact());
    }
}
