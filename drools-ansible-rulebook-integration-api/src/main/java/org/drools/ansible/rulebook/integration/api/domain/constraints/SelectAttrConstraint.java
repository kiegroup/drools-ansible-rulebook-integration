package org.drools.ansible.rulebook.integration.api.domain.constraints;

import java.util.Collection;
import java.util.Map;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;

import org.drools.ansible.rulebook.integration.api.domain.RuleGenerationContext;
import org.drools.ansible.rulebook.integration.api.domain.conditions.ConditionExpression;
import org.drools.ansible.rulebook.integration.api.rulesmodel.ParsedCondition;
import org.drools.model.ConstraintOperator;
import org.drools.model.Prototype;

import static org.drools.ansible.rulebook.integration.api.domain.conditions.ConditionExpression.map2Expr;
import static org.drools.ansible.rulebook.integration.api.domain.conditions.ConditionParseUtil.extractMapAttribute;
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
        return createSelectCondition(ruleContext, expression, expressionName.equals(EXPRESSION_NAME), true);
    }

    static ParsedCondition createSelectCondition(RuleGenerationContext ruleContext, Map<?, ?> expression, boolean positive, boolean withKey) {
        ConditionExpression left = map2Expr(ruleContext, expression.get("lhs"));
        ConstraintOperator operator = createSelectOperator((Map<?,?>) expression.get("rhs"), withKey);
        return new ParsedCondition(left.getPrototypeExpression(), operator, fixedValue(positive));
    }

    private static ConstraintOperator createSelectOperator(Map<?,?> rhs, boolean withKey) {
        String operator = ((Map<?,?>)rhs.get("operator")).entrySet().iterator().next().getValue().toString().trim();
        Object value = toJsonValue( rhs.get("value") );
        if (!withKey) {
            return new SelectAttrOperator( toOperatorPredicate(operator, value) );
        }
        String key = ((Map<?,?>)rhs.get("key")).entrySet().iterator().next().getValue().toString().trim();
        return new SelectAttrOperator( m -> extractMapAttribute((Map)m, key), toOperatorPredicate(operator, value) );
    }

    public static class SelectAttrOperator implements ConstraintOperator {

        private final Function keyExtractor;
        private final Predicate opPred;

        public SelectAttrOperator(Predicate opPred) {
            this( Function.identity(), opPred );
        }

        public SelectAttrOperator(Function keyExtractor, Predicate opPred) {
            this.keyExtractor = keyExtractor;
            this.opPred = opPred;
        }

        @Override
        public <T, V> BiPredicate<T, V> asPredicate() {
            return (t, v) -> {
                if (t instanceof Collection) {
                    return (boolean) v ?
                            ((Collection) t).stream().map(keyExtractor).anyMatch(opPred) :
                            !((Collection) t).stream().map(keyExtractor).allMatch(opPred);
                }
                Object value = keyExtractor.apply(t);
                return value != Prototype.UNDEFINED_VALUE && opPred.test(value) == (boolean) v;
            };
        }
    }
}