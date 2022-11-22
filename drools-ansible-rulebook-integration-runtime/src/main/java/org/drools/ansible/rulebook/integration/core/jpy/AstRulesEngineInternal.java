package org.drools.ansible.rulebook.integration.core.jpy;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.drools.ansible.rulebook.integration.api.RulesExecutor;
import org.drools.ansible.rulebook.integration.api.RulesExecutorContainer;
import org.drools.ansible.rulebook.integration.api.RulesExecutorFactory;
import org.drools.ansible.rulebook.integration.api.domain.RulesSet;
import org.kie.api.runtime.rule.Match;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

class AstRulesEngineInternal {

    public long createRuleset(RulesSet rulesSet) {
        RulesExecutor executor = RulesExecutorFactory.createRulesExecutor(rulesSet);
        return executor.getId();
    }

    public void dispose(long sessionId) {
        RulesExecutorContainer.INSTANCE.get(sessionId).dispose();
    }

    /**
     * @return error code (currently always 0)
     */
    public List<Map<String, ?>> retractFact(long sessionId, Map<String, Object> fact) {
        Map<String, Object> boundFact = Map.of("m", fact);
        List<Map<String, Map>> objs = processMessage(
                fact,
                RulesExecutorContainer.INSTANCE.get(sessionId)::processRetract);
        List<Map<String, ?>> results = objs.stream()
                .map(m -> m.entrySet().stream().findFirst()
                        .map(e -> Map.of(e.getKey(), boundFact)).get())
                .collect(Collectors.toList());
        return results;
    }

    public List<Map<String, Map>> assertFact(long sessionId, Map<String, Object> fact) {
        return processMessage(
                fact,
                RulesExecutorContainer.INSTANCE.get(sessionId)::processFacts);
    }

    public List<Map<String, Map>> assertEvent(long sessionId, Map<String, Object> fact) {
        return processMessage(
                fact,
                RulesExecutorContainer.INSTANCE.get(sessionId)::processEvents);
    }

    public List<Map<String, Object>> getFacts(long session_id) {
        return RulesExecutorContainer.INSTANCE.get(session_id).getAllFactsAsMap();
    }

    private List<Map<String, Map>> processMessage(Map<String, Object> fact, Function<Map<String, Object>, Collection<Match>> command) {
        return AstRuleMatch.asList(command.apply(fact));
    }

    private String toJson(Object elem) {
        try {
            return RulesExecutor.OBJECT_MAPPER.writeValueAsString(elem);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

    }
}
