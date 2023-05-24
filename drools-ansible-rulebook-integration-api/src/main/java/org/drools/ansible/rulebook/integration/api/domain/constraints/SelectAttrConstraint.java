package org.drools.ansible.rulebook.integration.api.domain.constraints;

import org.drools.ansible.rulebook.integration.api.domain.RuleGenerationContext;
import org.drools.ansible.rulebook.integration.api.domain.conditions.ConditionExpression;
import org.drools.ansible.rulebook.integration.api.rulesmodel.ParsedCondition;
import org.drools.model.ConstraintOperator;
import org.drools.model.Prototype;

import java.util.Collection;
import java.util.Map;
import java.util.function.BiPredicate;
import java.util.function.Function;

import static org.drools.ansible.rulebook.integration.api.domain.conditions.ConditionExpression.map2Expr;
import static org.drools.ansible.rulebook.integration.api.domain.conditions.ConditionParseUtil.extractMapAttribute;
import static org.drools.ansible.rulebook.integration.api.domain.conditions.ConditionParseUtil.mapToStringValue;
import static org.drools.ansible.rulebook.integration.api.domain.conditions.ConditionParseUtil.toJsonValue;
import static org.drools.ansible.rulebook.integration.api.domain.constraints.Operators.toOperatorPredicate;
import static org.drools.model.PrototypeExpression.fixedValue;

public enum SelectAttrConstraint implements ConstraintOperator, ConditionFactory {

    INSTANCE;

    public static final String EXPRESSION_NAME = "SelectAttrExpression";
    public static final String NEGATED_EXPRESSION_NAME = "SelectAttrNotExpression";

    @Override
    public <T, V> BiPredicate<T, V> asPredicate() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return "SELECT_ATTR";
    }

    @Override
    public ParsedCondition createParsedCondition(RuleGenerationContext ruleContext, String expressionName, Map<?, ?> expression) {
        boolean positive = expressionName.equals(EXPRESSION_NAME);
        Map<?,?> rhs = (Map<?,?>) expression.get("rhs");
        BiPredicate opPred = toOperatorPredicate( mapToStringValue(rhs.get("operator")) );
        ConditionExpression left = map2Expr(ruleContext, expression.get("lhs"));
        return createSelectAttrCondition(left, opPred, rhs, positive);
    }

    private static ParsedCondition createSelectAttrCondition(ConditionExpression left, BiPredicate opPred, Map<?, ?> rhs, boolean positive) {
        String key = mapToStringValue(rhs.get("key"));
        Object value = toJsonValue(rhs.get("value"));
        ConstraintOperator operator = new SelectAttrOperator(m -> extractMapAttribute((Map) m, key), opPred, positive);
        return new ParsedCondition(left.getPrototypeExpression(), operator, fixedValue(value));
    }

    private static class SelectAttrOperator implements ConstraintOperator {

        private final Function leftExtractor;
        private final BiPredicate opPred;
        private final boolean positive;


        public SelectAttrOperator(Function leftExtractor, BiPredicate opPred, boolean positive) {
            this.leftExtractor = leftExtractor;
            this.opPred = opPred;
            this.positive = positive;
        }

        @Override
        public <T, V> BiPredicate<T, V> asPredicate() {
            return (leftValue, rightValue) -> {
                if (leftValue instanceof Collection) {
                    return positive ?
                            ((Collection) leftValue).stream().anyMatch(left -> testPredicate(left, rightValue)) :
                            !((Collection) leftValue).stream().allMatch(left -> testPredicate(left, rightValue));
                }

                Object leftObject = leftExtractor.apply(leftValue);
                return leftObject != Prototype.UNDEFINED_VALUE && opPred.test(leftObject, rightValue) == positive;
            };
        }

        private <V> boolean testPredicate(Object left, V rightValue) {
            Object leftValue = leftExtractor.apply(left);
            return leftValue != Prototype.UNDEFINED_VALUE && opPred.test(leftValue, rightValue);
        }
    }
}