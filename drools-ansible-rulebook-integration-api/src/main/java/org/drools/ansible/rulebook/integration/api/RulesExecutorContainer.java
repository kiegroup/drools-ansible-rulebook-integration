package org.drools.ansible.rulebook.integration.api;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.drools.ansible.rulebook.integration.api.io.Response;
import org.drools.ansible.rulebook.integration.api.io.RuleExecutorChannel;

public class RulesExecutorContainer {

    private Map<Long, RulesExecutor> rulesExecutors = new ConcurrentHashMap<>();

    private RuleExecutorChannel channel;

    public RulesExecutorContainer(boolean async) {
        if (async) {
            channel = new RuleExecutorChannel();
        }
    }

    public RulesExecutor register(RulesExecutor rulesExecutor) {
        rulesExecutors.put(rulesExecutor.getId(), rulesExecutor);
        rulesExecutor.setRulesExecutorContainer(this);
        return rulesExecutor;
    }

    public void dispose(RulesExecutor rulesExecutor) {
        rulesExecutors.remove(rulesExecutor.getId());
    }

    public void disposeAll() {
        rulesExecutors.values().forEach(RulesExecutor::dispose);
        rulesExecutors.clear();
        if (channel != null) {
            channel.shutdown();
        }
    }

    public RulesExecutor get(Long id) {
        return rulesExecutors.get(id);
    }

    public void write(Response response) {
        channel.write(response);
    }

    public int port() {
        // used by client to know on which port the socket has been opened
        return channel.port();
    }
}
