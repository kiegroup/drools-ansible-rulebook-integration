package org.drools.ansible.rulebook.integration.durable.endpoint;

import org.drools.ansible.rulebook.integration.api.RulesExecutor;
import org.drools.ansible.rulebook.integration.api.RulesExecutorContainer;

public enum RulesExecutorContainerService {
    INSTANCE;

    private RulesExecutorContainer rulesExecutorContainer = new RulesExecutorContainer();

    public RulesExecutor get(Long id) {
        return rulesExecutorContainer.get(id);
    }

    public RulesExecutor register(RulesExecutor rulesExecutor) {
        return rulesExecutorContainer.register(rulesExecutor);
    }
}
