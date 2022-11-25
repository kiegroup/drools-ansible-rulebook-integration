package org.drools.ansible.rulebook.integration.core.jpy;

import java.util.Map;

import org.drools.ansible.rulebook.integration.api.RuleConfigurationOption;
import org.drools.ansible.rulebook.integration.api.RuleFormat;
import org.drools.ansible.rulebook.integration.api.RuleNotation;
import org.drools.ansible.rulebook.integration.api.domain.RulesSet;
import org.json.JSONObject;

import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.toJson;

public class AstRulesEngine {

    AstRulesEngineInternal internal = new AstRulesEngineInternal();

    public long createRuleset(String rulesetString) {
        return internal.createRuleset(RuleNotation.CoreNotation.INSTANCE.toRulesSet(RuleFormat.JSON, rulesetString));
    }

    public long createRulesetWithOptions(String rulesetString, boolean pseudoClock) {
        RulesSet rulesSet = RuleNotation.CoreNotation.INSTANCE.toRulesSet(RuleFormat.JSON, rulesetString);
        if (pseudoClock) rulesSet.withOptions(RuleConfigurationOption.USE_PSEUDO_CLOCK);
        return internal.createRuleset(rulesSet);
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

    public String getFacts(long sessionId) {
        return toJson(internal.getFacts(sessionId));
    }

    /**
     * Advances the clock time in the specified unit amount.
     *
     * @param amount the amount of units to advance in the clock
     * @param unit the used time unit
     * @return the events that fired
     */
    public String advanceTime(long sessionId, long amount, String unit) {
        return toJson(AstRuleMatch.asList(internal.advanceTime(sessionId, amount, unit)));
    }
}
