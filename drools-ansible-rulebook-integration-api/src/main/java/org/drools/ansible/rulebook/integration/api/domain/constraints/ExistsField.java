package org.drools.ansible.rulebook.integration.api.domain.constraints;

import java.util.Map;
import java.util.function.BiPredicate;

import org.drools.ansible.rulebook.integration.api.domain.RuleGenerationContext;
import org.drools.ansible.rulebook.integration.api.rulesmodel.ParsedCondition;
import org.drools.model.ConstraintOperator;
import org.drools.model.Prototype;

import static org.drools.ansible.rulebook.integration.api.domain.conditions.ConditionExpression.map2Expr;
import static org.drools.model.PrototypeExpression.thisPrototype;

public enum ExistsField implements ConstraintOperator, ConditionFactory {

    INSTANCE;

    public static final String EXPRESSION_NAME = "IsDefinedExpression";
    public static final String NEGATED_EXPRESSION_NAME = "IsNotDefinedExpression";

    @Override
    public <T, V> BiPredicate<T, V> asPredicate() {
        return (t, v) -> v != Prototype.UNDEFINED_VALUE; // actually, always true from caller: https://github.com/kiegroup/drools/blob/9de8d0b54b364bda1b1d76d81923c8bfc060c2f8/drools-model/drools-canonical-model/src/main/java/org/drools/model/PrototypeDSL.java#L273
    }

    @Override
    public String toString() {
        return "EXISTS_FIELD";
    }

    @Override
    public ParsedCondition createParsedCondition(RuleGenerationContext ruleContext, String expressionName, Map<?, ?> expression) {
        return new ParsedCondition(thisPrototype(), this, map2Expr(ruleContext, expression).getPrototypeExpression()).withNotPattern(expressionName.equals(NEGATED_EXPRESSION_NAME));
    }
}
