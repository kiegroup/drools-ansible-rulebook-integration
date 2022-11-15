package org.drools.ansible.rulebook.integration.core.jpy;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.drools.ansible.rulebook.integration.api.RulesExecutor;
import org.drools.ansible.rulebook.integration.api.RulesExecutorContainer;
import org.drools.ansible.rulebook.integration.api.RulesExecutorFactory;
import org.json.JSONObject;
import org.kie.api.runtime.rule.Match;

public class AstRulesEngine {

    public long createRuleset(String rulesetString) {
        RulesExecutor executor = RulesExecutorFactory.createFromJson(rulesetString);
        return executor.getId();
    }

    public void dispose(long sessionId) {
        RulesExecutorContainer.INSTANCE.get(sessionId).dispose();
    }

    /**
     * @return error code (currently always 0)
     */
    public String retractFact(long sessionId, String serializedFact) {
        Map<String, Object> fact = new JSONObject(serializedFact).toMap();
        Map<String, Object> boundFact = Map.of("m", fact);
        List<Map<String, Map>> objs = processMessage(
                serializedFact,
                RulesExecutorContainer.INSTANCE.get(sessionId)::processRetract);
        List<Map<String, ?>> results = objs.stream()
                .map(m -> m.entrySet().stream().findFirst()
                        .map(e -> Map.of(e.getKey(), boundFact)).get())
                .collect(Collectors.toList());
        return toJson(results);
    }

    public String assertFact(long sessionId, String serializedFact) {
        return toJson(processMessage(
                serializedFact,
                RulesExecutorContainer.INSTANCE.get(sessionId)::processFacts));
    }

    public String assertEvent(long sessionId, String serializedFact) {
        return toJson(processMessage(
                serializedFact,
                RulesExecutorContainer.INSTANCE.get(sessionId)::processEvents));
    }

    public String getFacts(long session_id) {
        return RulesExecutorContainer.INSTANCE.get(session_id).getAllFactsAsJson();
    }

    /**
     * Advances the clock time in the specified unit amount.
     *
     * @param amount the amount of units to advance in the clock
     * @param unit the used time unit
     * @return the events that fired
     */
    public String advanceTime(long sessionId, long amount, String unit) {
        return toJson(AstRuleMatch.asList(RulesExecutorContainer.INSTANCE.get(sessionId)
                .advanceTime(amount, TimeUnit.valueOf(unit.toUpperCase()))));
    }

    private List<Map<String, Map>> processMessage(String serializedFact, Function<String, Collection<Match>> command) {
        return AstRuleMatch.asList(command.apply(serializedFact));
    }

    private String toJson(Object elem) {
        try {
            return RulesExecutor.OBJECT_MAPPER.writeValueAsString(elem);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

    }
}
