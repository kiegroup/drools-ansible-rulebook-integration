package org.drools.ansible.rulebook.integration.api.domain.actions;

import org.drools.model.Drools;
import org.drools.ansible.rulebook.integration.api.RulesExecutor;

public interface Action {
    void execute(Drools drools);
}
