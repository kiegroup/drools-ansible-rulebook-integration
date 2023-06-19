package org.drools.ansible.rulebook.integration.api.domain.constraints;

import java.util.Map;
import java.util.function.BiPredicate;

import org.drools.ansible.rulebook.integration.api.domain.RuleGenerationContext;
import org.drools.ansible.rulebook.integration.api.domain.conditions.ConditionExpression;
import org.drools.ansible.rulebook.integration.api.rulesmodel.ParsedCondition;
import org.drools.model.ConstraintOperator;
import org.drools.model.Prototype;
import org.drools.model.PrototypeExpression;
import org.drools.model.PrototypeFact;

import static org.drools.ansible.rulebook.integration.api.domain.conditions.ConditionExpression.map2Expr;
import static org.drools.model.PrototypeExpression.fixedValue;
import static org.drools.model.PrototypeExpression.thisPrototype;

public enum ExistsField implements ConstraintOperator, ConditionFactory {

    INSTANCE;

    public static final String EXPRESSION_NAME = "IsDefinedExpression";
    public static final String NEGATED_EXPRESSION_NAME = "IsNotDefinedExpression";

    @Override
    public <T, V> BiPredicate<T, V> asPredicate() {
        throw new UnsupportedOperationException("converted to make use of "+ExistsFieldUsingExtractor.class.getCanonicalName());
    }

    @Override
    public String toString() {
        return "EXISTS_FIELD";
    }

    @Override
    public ParsedCondition createParsedCondition(RuleGenerationContext ruleContext, String expressionName, Map<?, ?> expression) {
        ConditionExpression map2Expr = map2Expr(ruleContext, expression);
        PrototypeExpression usedProtototypeExpr = map2Expr.getPrototypeExpression();
        PrototypeExpression unused = fixedValue(map2Expr.getFieldName());
        return new ParsedCondition(thisPrototype(), new ExistsFieldUsingExtractor(usedProtototypeExpr), unused).withNotPattern(expressionName.equals(NEGATED_EXPRESSION_NAME));
    }

    public static class ExistsFieldUsingExtractor implements ConstraintOperator {
        private final PrototypeExpression expr;

        public ExistsFieldUsingExtractor(PrototypeExpression expr) {
            this.expr = expr;
        }

        @Override
        public <T, V> BiPredicate<T, V> asPredicate() {
            return (t, v) -> expr.asFunction(null).apply((PrototypeFact) t) != Prototype.UNDEFINED_VALUE;
        }

    }
}
