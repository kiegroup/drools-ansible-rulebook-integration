package org.drools.ansible.rulebook.integration.api.domain.conditions;

import java.util.Collections;
import java.util.List;

import org.drools.ansible.rulebook.integration.api.domain.RuleGenerationContext;
import org.drools.model.Drools;
import org.drools.model.Rule;
import org.drools.model.RuleItemBuilder;
import org.drools.model.Variable;
import org.drools.model.view.ViewItem;

import static org.drools.model.PatternDSL.rule;

public interface TimeConstraint {

    String GROUP_BY_ATTRIBUTES = "group_by_attributes";

    boolean requiresAsyncExecution();

    ViewItem processTimeConstraint(ViewItem pattern);

    default Variable<?>[] getTimeConstraintConsequenceVariables() {
        return null;
    }

    default void executeTimeConstraintConsequence(Drools drools, Object... facts) { }

    default Rule buildTimedRule(String ruleName, RuleItemBuilder pattern, RuleItemBuilder consequence) {
        return rule( ruleName ).build( pattern, consequence );
    }

    default List<Rule> getControlRules(RuleGenerationContext ruleContext) {
        return Collections.emptyList();
    }
}
