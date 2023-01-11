package org.drools.ansible.rulebook.integration.api.domain.conditions;

import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;

import org.drools.ansible.rulebook.integration.api.domain.RuleGenerationContext;
import org.drools.model.Drools;
import org.drools.model.PrototypeFact;
import org.drools.model.PrototypeVariable;
import org.drools.model.Rule;
import org.drools.model.RuleItemBuilder;
import org.drools.model.view.ViewItem;

import static org.drools.model.PatternDSL.rule;

public interface TimeConstraint {

    String GROUP_BY_ATTRIBUTES = "group_by_attributes";

    ViewItem processTimeConstraint(ViewItem pattern);

    default PrototypeVariable getTimeConstraintConsequenceVariable() {
        return null;
    }

    default BiConsumer<Drools, PrototypeFact> getTimeConstraintConsequence() {
        return (Drools drools, PrototypeFact fact) -> { };
    }

    default Rule buildTimedRule(String ruleName, RuleItemBuilder pattern, RuleItemBuilder consequence) {
        return rule( ruleName ).build( pattern, consequence );
    }

    default List<Rule> getControlRules(RuleGenerationContext ruleContext) {
        return Collections.emptyList();
    }
}
