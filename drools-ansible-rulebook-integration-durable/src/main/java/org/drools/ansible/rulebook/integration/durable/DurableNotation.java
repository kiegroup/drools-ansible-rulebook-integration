package org.drools.ansible.rulebook.integration.durable;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.drools.ansible.rulebook.integration.api.RuleNotation;
import org.drools.ansible.rulebook.integration.api.domain.RulesSet;
import org.drools.ansible.rulebook.integration.durable.domain.DurableRules;

public enum DurableNotation implements RuleNotation {

    INSTANCE;

    @Override
    public RulesSet jsonToRuleSet(ObjectMapper mapper, String json) {
        try {
            return mapper.readValue(json, DurableRules.class ).toRulesSet();
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
