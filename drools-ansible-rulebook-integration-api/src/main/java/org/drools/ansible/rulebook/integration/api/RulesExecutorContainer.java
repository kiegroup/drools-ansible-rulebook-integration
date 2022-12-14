package org.drools.ansible.rulebook.integration.api;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.drools.ansible.rulebook.integration.api.io.RuleExecutorChannel;
import org.drools.ansible.rulebook.integration.api.rulesengine.AsyncExecutor;

public class RulesExecutorContainer {

    private Map<Long, RulesExecutor> rulesExecutors = new ConcurrentHashMap<>();

    private AsyncExecutor asyncExecutor;
    private RuleExecutorChannel channel;

    public RulesExecutorContainer allowAsync() {
        if (asyncExecutor == null) {
            asyncExecutor = new AsyncExecutor();
            channel = new RuleExecutorChannel().accept(asyncExecutor);
        }
        return this;
    }

    public RulesExecutor register(RulesExecutor rulesExecutor) {
        rulesExecutors.put(rulesExecutor.getId(), rulesExecutor);
        rulesExecutor.setRulesExecutorContainer(this);
        return rulesExecutor;
    }

    public void dispose(long rulesExecutorId) {
        rulesExecutors.remove(rulesExecutorId);
    }

    public void disposeAll() {
        rulesExecutors.values().forEach(RulesExecutor::dispose);
        rulesExecutors.clear();
        if (channel != null) {
            asyncExecutor.shutdown();
            channel.shutdown();
        }
    }

    public RulesExecutor get(Long id) {
        return rulesExecutors.get(id);
    }

    public AsyncExecutor getAsyncExecutor() {
        return asyncExecutor;
    }

    public RuleExecutorChannel getChannel() {
        return channel;
    }

    public int port() {
        // used by client to know on which port the socket has been opened
        if (channel == null) {
            // TODO: for now always open the channel when a port is asked, in reality it should
            // throw new IllegalStateException("Channel not available. Is there any async communication required?");
            allowAsync();
        }
        return channel.port();
    }
}
