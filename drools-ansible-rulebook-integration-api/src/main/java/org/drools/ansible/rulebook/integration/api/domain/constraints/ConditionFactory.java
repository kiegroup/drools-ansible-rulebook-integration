package org.drools.ansible.rulebook.integration.api.domain.constraints;

import java.util.Map;

import org.drools.ansible.rulebook.integration.api.domain.RuleGenerationContext;
import org.drools.ansible.rulebook.integration.api.rulesmodel.ParsedCondition;

public interface ConditionFactory {
    ParsedCondition createParsedCondition(RuleGenerationContext ruleContext, String expressionName, Map<?, ?> expression);
}
