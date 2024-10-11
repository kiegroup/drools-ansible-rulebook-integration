package org.drools.ansible.rulebook.integration.api.domain.constraints;

import java.util.Map;
import java.util.function.BiPredicate;

import org.drools.ansible.rulebook.integration.api.domain.RuleGenerationContext;
import org.drools.model.Index;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.drools.ansible.rulebook.integration.api.LogUtil.convertJavaClassToPythonClass;
import static org.drools.model.util.OperatorUtils.areEqual;
import static org.drools.model.util.OperatorUtils.compare;

public class RulebookConstraintOperator implements RulebookOperator {

    private static final Logger LOG = LoggerFactory.getLogger(RulebookConstraintOperator.class);

    private Index.ConstraintType type;
    private ConditionContext conditionContext;
    private boolean typeCheckLogged = false;

    public RulebookConstraintOperator(Index.ConstraintType type) {
        this.type = type;
    }

    @Override
    public void setConditionContext(RuleGenerationContext ruleContext, Map<?, ?> expression) {
        this.conditionContext = new ConditionContext(ruleContext.getRuleSetName(), ruleContext.getRuleName(), expression.toString());
    }

    @Override
    public boolean hasIndex() {
        return true;
    }

    @Override
    public Index.ConstraintType getIndexType() {
        return type;
    }

    public RulebookConstraintOperator negate() {
        switch (this.type) {
            case FORALL_SELF_JOIN:
            case EQUAL:
                this.type = Index.ConstraintType.NOT_EQUAL;
                return this;
            case NOT_EQUAL:
                this.type = Index.ConstraintType.EQUAL;
                return this;
            case GREATER_THAN:
                this.type = Index.ConstraintType.LESS_OR_EQUAL;
                return this;
            case GREATER_OR_EQUAL:
                this.type = Index.ConstraintType.LESS_THAN;
                return this;
            case LESS_OR_EQUAL:
                this.type = Index.ConstraintType.GREATER_THAN;
                return this;
            case LESS_THAN:
                this.type = Index.ConstraintType.GREATER_OR_EQUAL;
                return this;
        }
        this.type = Index.ConstraintType.UNKNOWN;
        return this;
    }

    @Override
    public boolean canInverse() {
        switch (this.type) {
            case EQUAL:
            case NOT_EQUAL:
            case GREATER_THAN:
            case GREATER_OR_EQUAL:
            case LESS_THAN:
            case LESS_OR_EQUAL:
                return true;
            default:
                return false;
        }
    }

    @Override
    public <T, V> BiPredicate<T, V> asPredicate() {
        final BiPredicate<T, V> predicate;
        switch (this.type) {
            case EQUAL:
                predicate = (t, v) -> areEqual(t, v);
                break;
            case NOT_EQUAL:
                predicate = (t, v) -> !areEqual(t, v);
                break;
            case GREATER_THAN:
                predicate = (t,v) -> t != null && compare(t, v) > 0;
                break;
            case GREATER_OR_EQUAL:
                predicate = (t,v) -> t != null && compare(t, v) >= 0;
                break;
            case LESS_THAN:
                predicate = (t,v) -> t != null && compare(t, v) < 0;
                break;
            case LESS_OR_EQUAL:
                predicate = (t,v) -> t != null && compare(t, v) <= 0;
                break;
            default:
                throw new UnsupportedOperationException("Cannot convert " + this + " into a predicate");
        }
        return (t, v) -> predicateWithTypeCheck(t, v, predicate);
    }

    private <T, V> boolean predicateWithTypeCheck(T t, V v, BiPredicate<T, V> predicate) {
        if (isCompatibleType(t, v)) {
            return predicate.test(t, v);
        } else {
            logTypeCheck(t, v);
            return false; // Different types are never equal
        }
    }

    /*
     * Log a type check error once per constraint
     */
    <T, V> void logTypeCheck(T t, V v) {
        if (!typeCheckLogged) {
            LOG.error("Cannot compare values of different types: {} and {}. RuleSet: {}. RuleName: {}. Condition: {}",
                      convertJavaClassToPythonClass(t.getClass()),
                      convertJavaClassToPythonClass(v.getClass()),
                      conditionContext.getRuleSet(), conditionContext.getRuleName(), conditionContext.getConditionExpression());
            typeCheckLogged = true; // Log only once per constraint
        }
    }

    public static boolean isCompatibleType(Object t, Object v) {
        return t == null
                || v == null
                || t instanceof Number && v instanceof Number
                || t.getClass() == v.getClass();
    }

    @Override
    public RulebookConstraintOperator inverse() {
        switch (this.type) {
            case GREATER_THAN:
                this.type = Index.ConstraintType.LESS_THAN;
                return this;
            case GREATER_OR_EQUAL:
                this.type =  Index.ConstraintType.LESS_OR_EQUAL;
                return this;
            case LESS_THAN:
                this.type = Index.ConstraintType.GREATER_THAN;
                return this;
            case LESS_OR_EQUAL:
                this.type = Index.ConstraintType.GREATER_OR_EQUAL;
                return this;
            default:
                return this;
        }
    }

    public boolean isComparison() {
        return isAscending() || isDescending();
    }

    public boolean isAscending() {
        return this.type == Index.ConstraintType.GREATER_THAN || this.type == Index.ConstraintType.GREATER_OR_EQUAL;
    }

    public boolean isDescending() {
        return this.type == Index.ConstraintType.LESS_THAN || this.type == Index.ConstraintType.LESS_OR_EQUAL;
    }

    private class ConditionContext {

        private String ruleSet;
        private String ruleName;
        private String conditionExpression;

        public ConditionContext(String ruleSet, String ruleName, String conditionExpression) {
            this.ruleSet = ruleSet;
            this.ruleName = ruleName;
            this.conditionExpression = conditionExpression;
        }

        public String getRuleSet() {
            return ruleSet;
        }

        public String getRuleName() {
            return ruleName;
        }

        public String getConditionExpression() {
            return conditionExpression;
        }
    }

    @Override
    public String toString() {
        // works for node sharing
        return type.toString();
    }
}
