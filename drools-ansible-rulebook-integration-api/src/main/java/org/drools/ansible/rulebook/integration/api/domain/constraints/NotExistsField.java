package org.drools.ansible.rulebook.integration.api.domain.constraints;

import java.util.Map;
import java.util.function.BiPredicate;

import org.drools.ansible.rulebook.integration.api.domain.RuleGenerationContext;
import org.drools.ansible.rulebook.integration.api.rulesmodel.ParsedCondition;
import org.drools.model.ConstraintOperator;
import org.drools.model.PrototypeFact;

import static org.drools.ansible.rulebook.integration.api.domain.conditions.ConditionExpression.map2Expr;
import static org.drools.model.PrototypeExpression.fixedValue;
import static org.drools.model.PrototypeExpression.thisPrototype;

public enum NotExistsField implements ConstraintOperator, ConditionFactory {

    INSTANCE;

    public static final String EXPRESSION_NAME = "IsNotDefinedExpression";

    @Override
    public <T, V> BiPredicate<T, V> asPredicate() {
        return (t,v) -> !((PrototypeFact) t).has((String) v);
    }

    @Override
    public String toString() {
        return "NOT_EXISTS_FIELD";
    }

    @Override
    public ParsedCondition createParsedCondition(RuleGenerationContext ruleContext, String expressionName, Map<?, ?> expression) {
        return new ParsedCondition(thisPrototype(), this, fixedValue(map2Expr(ruleContext, expression).getFieldName()));
    }
}
