package org.drools.ansible.rulebook.integration.api.domain.actions;

import org.drools.model.Drools;
import org.drools.ansible.rulebook.integration.api.RulesExecutor;

import static org.drools.ansible.rulebook.integration.api.rulesmodel.RulesModelUtil.mapToFact;

public class AssertFact extends FactAction {

    static final String ACTION_NAME = "assert_fact";

    @Override
    public String toString() {
        return "AssertFact{" +
                "ruleset='" + getRuleset() + '\'' +
                ", fact=" + getFact() +
                '}';
    }

    @Override
    public void execute(Drools drools) {
        System.out.println("Asserting " + getFact());
        drools.insert(mapToFact(getFact()));
    }
}
