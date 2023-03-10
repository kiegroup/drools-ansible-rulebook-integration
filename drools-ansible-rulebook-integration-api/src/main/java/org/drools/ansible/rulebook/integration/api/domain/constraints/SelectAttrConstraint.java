package org.drools.ansible.rulebook.integration.api.domain.constraints;

import java.util.Collection;
import java.util.Map;
import java.util.function.BiPredicate;
import java.util.function.Function;

import org.drools.ansible.rulebook.integration.api.domain.RuleGenerationContext;
import org.drools.ansible.rulebook.integration.api.domain.conditions.ConditionExpression;
import org.drools.ansible.rulebook.integration.api.rulesmodel.ParsedCondition;
import org.drools.core.facttemplates.Fact;
import org.drools.model.ConstraintOperator;
import org.drools.model.Prototype;

import static org.drools.ansible.rulebook.integration.api.domain.conditions.ConditionExpression.map2Expr;
import static org.drools.ansible.rulebook.integration.api.domain.conditions.ConditionParseUtil.extractMapAttribute;
import static org.drools.ansible.rulebook.integration.api.domain.conditions.ConditionParseUtil.isEventOrFact;
import static org.drools.ansible.rulebook.integration.api.domain.conditions.ConditionParseUtil.toJsonValue;
import static org.drools.ansible.rulebook.integration.api.domain.constraints.Operators.toOperatorPredicate;
import static org.drools.model.PrototypeExpression.fixedValue;
import static org.drools.model.PrototypeExpression.thisPrototype;

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
        return createSelectCondition(ruleContext, expression, expressionName.equals(EXPRESSION_NAME), true);
    }

    static ParsedCondition createSelectCondition(RuleGenerationContext ruleContext, Map<?, ?> expression, boolean positive, boolean withKey) {
        Map<?,?> rhs = (Map<?,?>) expression.get("rhs");
        BiPredicate opPred = toOperatorPredicate( mapToStringValue(rhs.get("operator")) );

        if (!withKey) {
            Object rhsValue = rhs.get("value");
            if (rhsValue instanceof Map && isEventOrFact(((Map) rhsValue).keySet().iterator().next().toString())) {
                return createSelectConditionWithLeftAndRightFields(expression, opPred, rhsValue, positive);
            }
        }

        ConditionExpression left = map2Expr(ruleContext, expression.get("lhs"));
        return withKey ?
                createSelectAttrCondition(left, opPred, rhs, positive) :
                createSelectConditionWithFixedRight(left, opPred, rhs.get("value"), positive);
    }

    private static ParsedCondition createSelectConditionWithLeftAndRightFields(Map<?, ?> expression, BiPredicate opPred, Object rhsValue, boolean positive) {
        String leftField = mapToStringValue(expression.get("lhs"));
        String rightField = mapToStringValue(rhsValue);
        ConstraintOperator operator = new SelectFieldOperator(f -> ((Fact) f).get(leftField), opPred, f -> ((Fact) f).get(rightField));
        return new ParsedCondition(thisPrototype(), operator, fixedValue(positive));
    }

    private static ParsedCondition createSelectConditionWithFixedRight(ConditionExpression left, BiPredicate opPred, Object rhsValue, boolean positive) {
        ConstraintOperator operator = new SelectAttrOperator(opPred, toJsonValue(rhsValue));
        return new ParsedCondition(left.getPrototypeExpression(), operator, fixedValue(positive));
    }

    private static ParsedCondition createSelectAttrCondition(ConditionExpression left, BiPredicate opPred, Map<?, ?> rhs, boolean positive) {
        String key = mapToStringValue(rhs.get("key"));
        Object value = toJsonValue(rhs.get("value"));
        ConstraintOperator operator = new SelectAttrOperator(m -> extractMapAttribute((Map) m, key), opPred, value);
        return new ParsedCondition(left.getPrototypeExpression(), operator, fixedValue(positive));
    }

    private static String mapToStringValue(Object rhsValue) {
        Map<?,?> rhsMap = (Map<?,?>)rhsValue;
        if (rhsMap.size() != 1) {
            throw new UnsupportedOperationException("The map " + rhsMap + " must have exactly 1 entry");
        }
        return rhsMap.entrySet().iterator().next().getValue().toString().trim();
    }

    public static class SelectAttrOperator implements ConstraintOperator {

        private final Function leftExtractor;
        private final BiPredicate opPred;
        private final Object rightValue;

        public SelectAttrOperator(BiPredicate opPred, Object rightValue) {
            this( Function.identity(), opPred, rightValue );
        }

        public SelectAttrOperator(Function leftExtractor, BiPredicate opPred, Object rightValue) {
            this.leftExtractor = leftExtractor;
            this.opPred = opPred;
            this.rightValue = rightValue;
        }

        @Override
        public <T, V> BiPredicate<T, V> asPredicate() {
            return (t, v) -> {
                if (t instanceof Collection) {
                    return (boolean) v ?
                            ((Collection) t).stream().anyMatch(left -> opPred.test(leftExtractor.apply(left), rightValue)) :
                            !((Collection) t).stream().allMatch(left -> opPred.test(leftExtractor.apply(left), rightValue));
                }

                Object leftValue = leftExtractor.apply(t);
                return leftValue != Prototype.UNDEFINED_VALUE && opPred.test(leftValue, rightValue) == (boolean) v;
            };
        }
    }

    public static class SelectFieldOperator implements ConstraintOperator {
        private final Function leftExtractor;
        private final BiPredicate opPred;
        private final Function rightExtractor;

        public SelectFieldOperator(Function leftExtractor, BiPredicate opPred, Function rightExtractor) {
            this.leftExtractor = leftExtractor;
            this.opPred = opPred;
            this.rightExtractor = rightExtractor;
        }

        @Override
        public <T, V> BiPredicate<T, V> asPredicate() {
            return (t, v) -> {
                Object leftValue = leftExtractor.apply(t);
                Object rightValue = rightExtractor.apply(t);

                if (leftValue instanceof Collection) {
                    return (boolean) v ?
                            ((Collection) leftValue).stream().anyMatch(x -> opPred.test(x, rightValue)) :
                            !((Collection) leftValue).stream().allMatch(x -> opPred.test(x, rightValue));
                }

                return leftValue != Prototype.UNDEFINED_VALUE && opPred.test(leftValue, rightValue) == (boolean) v;
            };
        }
    }
}