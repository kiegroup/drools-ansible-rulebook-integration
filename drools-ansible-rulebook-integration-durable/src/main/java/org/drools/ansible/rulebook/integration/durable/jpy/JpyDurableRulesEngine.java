package org.drools.ansible.rulebook.integration.durable.jpy;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.drools.ansible.rulebook.integration.api.RulesExecutor;
import org.drools.ansible.rulebook.integration.api.RulesExecutorContainer;
import org.drools.ansible.rulebook.integration.api.RulesExecutorFactory;
import org.drools.ansible.rulebook.integration.durable.DurableNotation;
import org.drools.ansible.rulebook.integration.durable.domain.DurableRuleMatch;
import org.kie.api.runtime.rule.Match;

import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.toJson;

public class JpyDurableRulesEngine {

    private final RulesExecutorContainer rulesExecutorContainer = new RulesExecutorContainer(false);

    private Iterator<Map<String, Map>> lastResponse = Collections.emptyIterator();

    public long createRuleset(String ruleSetName, String rulesetString) {
        RulesExecutor executor = RulesExecutorFactory.createFromJson(
                DurableNotation.INSTANCE,
                String.format("{\"%s\":%s}", ruleSetName, rulesetString));
        rulesExecutorContainer.register(executor);
        return executor.getId();
    }

    /**
     * @return error code (currently always 0)
     */
    public int retractFact(long sessionId, String serializedFact) {
        rulesExecutorContainer.get(sessionId).processRetract(serializedFact);
        return 0;
    }

    public String advanceState() {
        if (lastResponse.hasNext()) {
            Map<String, Map> elem = lastResponse.next();
            return toJson(elem);
        }
        return null;
    }

    public int assertFact(long sessionId, String serializedFact) {
        return processMessage(
                serializedFact,
                rulesExecutorContainer.get(sessionId)::processFacts);
    }

    public int assertEvent(long sessionId, String serializedFact) {
        return processMessage(
                serializedFact,
                rulesExecutorContainer.get(sessionId)::processEvents);
    }

    public String getFacts(long session_id) {
        return rulesExecutorContainer.get(session_id).getAllFactsAsJson();
    }

    private int processMessage(String serializedFact, Function<String, Collection<Match>> command) {
        List<Map<String, Map>> lastResponse = DurableRuleMatch.asList(command.apply(serializedFact));
        this.lastResponse = lastResponse.iterator();
        return 0;
    }
}
