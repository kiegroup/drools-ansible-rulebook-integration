package org.drools.ansible.rulebook.integration.core.jpy;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.drools.ansible.rulebook.integration.api.RuleConfigurationOption;
import org.drools.ansible.rulebook.integration.api.RuleFormat;
import org.drools.ansible.rulebook.integration.api.RuleNotation;
import org.drools.ansible.rulebook.integration.api.RulesExecutor;
import org.drools.ansible.rulebook.integration.api.RulesExecutorContainer;
import org.drools.ansible.rulebook.integration.api.RulesExecutorFactory;
import org.json.JSONObject;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class AstRulesEngine {

    AstRulesEngineInternal internal = new AstRulesEngineInternal();

    public long createRuleset(String rulesetString) {
        return internal.createRuleset(RuleNotation.CoreNotation.INSTANCE.toRulesSet(RuleFormat.JSON, rulesetString));
    }

    public long createRulesetWithOptions(String rulesetString, boolean pseudoClock) {
        RuleNotation notation = RuleNotation.CoreNotation.INSTANCE;
        if (pseudoClock) {
            notation = notation.withOptions(RuleConfigurationOption.USE_PSEUDO_CLOCK);
        }
        RulesExecutor executor = RulesExecutorFactory.createFromJson(notation, rulesetString);
        return executor.getId();
    }

    public void dispose(long sessionId) {
        internal.dispose(sessionId);
    }

    /**
     * @return error code (currently always 0)
     */
    public String retractFact(long sessionId, String serializedFact) {
        Map<String, Object> fact = new JSONObject(serializedFact).toMap();
        return toJson(internal.retractFact(sessionId, fact));
    }

    public String assertFact(long sessionId, String serializedFact) {
        Map<String, Object> fact = new JSONObject(serializedFact).toMap();
        return toJson(internal.assertFact(sessionId, fact));
    }

    public String assertEvent(long sessionId, String serializedFact) {
        Map<String, Object> fact = new JSONObject(serializedFact).toMap();
        return toJson(internal.assertEvent(sessionId, fact));
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

    private String toJson(Object elem) {
        try {
            return RulesExecutor.OBJECT_MAPPER.writeValueAsString(elem);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

    }
}
