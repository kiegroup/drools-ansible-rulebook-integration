package org.drools.ansible.rulebook.integration.api;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public enum RulesExecutorContainer {

    INSTANCE;

    private Map<Long, RulesExecutor> rulesExecutors = new ConcurrentHashMap<>();

    public RulesExecutor register(RulesExecutor rulesExecutor) {
        rulesExecutors.put(rulesExecutor.getId(), rulesExecutor);
        return rulesExecutor;
    }

    public void dispose(RulesExecutor rulesExecutor) {
        rulesExecutors.remove(rulesExecutor.getId());
    }

    public void disposeAll() {
        ArrayList<Long> sessionIds = new ArrayList<>(rulesExecutors.keySet());
        for (long id : sessionIds) {
            RulesExecutor executor = rulesExecutors.remove(id);
            executor.dispose();
        }
    }

    public RulesExecutor get(Long id) {
        return rulesExecutors.get(id);
    }
}
