package org.drools.ansible.rulebook.integration.api.domain.constraints;

import java.util.Map;
import java.util.function.BiPredicate;

import org.drools.ansible.rulebook.integration.api.domain.RuleGenerationContext;
import org.drools.ansible.rulebook.integration.api.rulesmodel.ParsedCondition;
import org.drools.model.ConstraintOperator;

import static org.drools.ansible.rulebook.integration.api.domain.constraints.SelectAttrConstraint.createSelectCondition;

public enum SelectConstraint implements ConstraintOperator, ConditionFactory {

    INSTANCE;

    public static final String EXPRESSION_NAME = "SelectExpression";
    public static final String NEGATED_EXPRESSION_NAME = "SelectNotExpression";

    @Override
    public <T, V> BiPredicate<T, V> asPredicate() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return "SELECT";
    }

    @Override
    public ParsedCondition createParsedCondition(RuleGenerationContext ruleContext, String expressionName, Map<?, ?> expression) {
        return createSelectCondition(ruleContext, expression, expressionName.equals(EXPRESSION_NAME), false);
    }
}