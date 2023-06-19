package org.drools.ansible.rulebook.integration.api.domain.constraints;

import java.util.Collection;
import java.util.Map;
import java.util.function.BiPredicate;

import org.drools.ansible.rulebook.integration.api.domain.RuleGenerationContext;
import org.drools.ansible.rulebook.integration.api.domain.conditions.ConditionExpression;
import org.drools.ansible.rulebook.integration.api.rulesmodel.BetaParsedCondition;
import org.drools.ansible.rulebook.integration.api.rulesmodel.ParsedCondition;
import org.drools.ansible.rulebook.integration.protoextractor.prototype.ExtractorPrototypeExpressionUtils;
import org.drools.model.ConstraintOperator;
import org.drools.model.Prototype;
import org.drools.model.PrototypeDSL;
import org.drools.model.PrototypeVariable;

import static org.drools.ansible.rulebook.integration.api.domain.conditions.ConditionExpression.map2Expr;
import static org.drools.ansible.rulebook.integration.api.domain.conditions.ConditionParseUtil.isEventOrFact;
import static org.drools.ansible.rulebook.integration.api.domain.conditions.ConditionParseUtil.mapToStringValue;
import static org.drools.ansible.rulebook.integration.api.domain.conditions.ConditionParseUtil.toJsonValue;
import static org.drools.ansible.rulebook.integration.api.domain.constraints.Operators.toOperatorPredicate;
import static org.drools.model.PrototypeExpression.fixedValue;
import static org.drools.model.PrototypeExpression.prototypeField;

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
        boolean positive = expressionName.equals(EXPRESSION_NAME);
        Map<?,?> rhs = (Map<?,?>) expression.get("rhs");
        BiPredicate opPred = toOperatorPredicate( mapToStringValue(rhs.get("operator")) );

        Object rhsValue = rhs.get("value");
        if (rhsValue instanceof Map && isEventOrFact(((Map) rhsValue).keySet().iterator().next().toString())) {
            return createSelectConditionWithLeftAndRightFields(ruleContext, expression, opPred, rhsValue, positive);
        }

        ConditionExpression left = map2Expr(ruleContext, expression.get("lhs"));
        return createSelectConditionWithFixedRight(left, opPred, rhs.get("value"), positive);
    }

    private static ParsedCondition createSelectConditionWithLeftAndRightFields(RuleGenerationContext ruleContext, Map<?, ?> expression, BiPredicate opPred, Object rhsValue, boolean positive) {
        String leftField = mapToStringValue(expression.get("lhs"));
        ConstraintOperator operator = new SelectFieldOperator(opPred, positive);
        String rightField = mapToStringValue(rhsValue);

        int dotPos = rightField.indexOf('.');
        String rightPatternBindName = dotPos < 0 ? rightField : rightField.substring(0, dotPos);
        PrototypeDSL.PrototypePatternDef rightPattern = ruleContext.getBoundPattern(rightPatternBindName);

        return rightPattern == null ?
                new ParsedCondition(ExtractorPrototypeExpressionUtils.prototypeFieldExtractor(leftField), operator, ExtractorPrototypeExpressionUtils.prototypeFieldExtractor(rightField)) :
                new BetaParsedCondition(ExtractorPrototypeExpressionUtils.prototypeFieldExtractor(leftField), operator, (PrototypeVariable) rightPattern.getFirstVariable(), ExtractorPrototypeExpressionUtils.prototypeFieldExtractorSkippingFirst(rightField));
    }

    static ParsedCondition createSelectConditionWithFixedRight(ConditionExpression left, BiPredicate opPred, Object rhsValue, boolean positive) {
        ConstraintOperator operator = new SelectAttrOperator(opPred, toJsonValue(rhsValue), positive);
        return new ParsedCondition(left.getPrototypeExpression(), operator, fixedValue(positive));
    }

    private static class SelectFieldOperator implements ConstraintOperator {
        private final BiPredicate opPred;
        private final boolean positive;

        public SelectFieldOperator(BiPredicate opPred, boolean positive) {
            this.opPred = opPred;
            this.positive = positive;
        }

        @Override
        public <T, V> BiPredicate<T, V> asPredicate() {
            return (leftValue, rightValue) -> evaluateSelect(leftValue, opPred, rightValue, positive);
        }
    }

    private static class SelectAttrOperator implements ConstraintOperator {

        private final BiPredicate opPred;
        private final Object rightValue;
        private final boolean positive;


        public SelectAttrOperator(BiPredicate opPred, Object rightValue, boolean positive) {
            this.opPred = opPred;
            this.rightValue = rightValue;
            this.positive = positive;
        }

        @Override
        public <T, V> BiPredicate<T, V> asPredicate() {
            return (leftValue, ignored) -> evaluateSelect(leftValue, opPred, rightValue, positive);
        }
    }

    private static <T, V> boolean evaluateSelect(T leftValue, BiPredicate opPred, V rightValue, boolean positive) {
        if (leftValue instanceof Collection) {
            return positive ?
                    ((Collection) leftValue).stream().anyMatch(x -> opPred.test(x, rightValue)) :
                    !((Collection) leftValue).stream().allMatch(x -> opPred.test(x, rightValue));
        }

        return leftValue != Prototype.UNDEFINED_VALUE && opPred.test(leftValue, rightValue) == positive;
    }
}