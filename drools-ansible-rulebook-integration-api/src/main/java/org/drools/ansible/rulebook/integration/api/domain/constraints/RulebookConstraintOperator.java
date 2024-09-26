package org.drools.ansible.rulebook.integration.api.domain.constraints;

import java.util.Map;
import java.util.function.BiPredicate;

import org.drools.ansible.rulebook.integration.api.domain.RuleGenerationContext;
import org.drools.model.ConstraintOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.drools.model.util.OperatorUtils.areEqual;
import static org.drools.model.util.OperatorUtils.compare;

public class RulebookConstraintOperator implements ConstraintOperator {

    enum RulebookConstraintOperatorType {
        EQUAL,
        NOT_EQUAL,
        GREATER_THAN,
        GREATER_OR_EQUAL,
        LESS_THAN,
        LESS_OR_EQUAL,
        UNKNOWN;
    }

    private static final Logger LOG = LoggerFactory.getLogger(RulebookConstraintOperator.class);

    private RulebookConstraintOperatorType type;
    private ConditionContext conditionContext;
    private boolean typeCheckLogged = false;

    public RulebookConstraintOperator(RulebookConstraintOperatorType type) {
        this.type = type;
    }

    public void setConditionContext(RuleGenerationContext ruleContext, Map<?, ?> expression) {
        this.conditionContext = new ConditionContext(ruleContext.getRuleSetName(), ruleContext.getRuleName(), expression.toString());
    }

    public RulebookConstraintOperator negate() {
        switch (this.type) {
            case EQUAL:
                this.type = RulebookConstraintOperatorType.NOT_EQUAL;
                return this;
            case NOT_EQUAL:
                this.type = RulebookConstraintOperatorType.EQUAL;
                return this;
            case GREATER_THAN:
                this.type = RulebookConstraintOperatorType.LESS_OR_EQUAL;
                return this;
            case GREATER_OR_EQUAL:
                this.type = RulebookConstraintOperatorType.LESS_THAN;
                return this;
            case LESS_OR_EQUAL:
                this.type = RulebookConstraintOperatorType.GREATER_THAN;
                return this;
            case LESS_THAN:
                this.type = RulebookConstraintOperatorType.GREATER_OR_EQUAL;
                return this;
        }
        this.type = RulebookConstraintOperatorType.UNKNOWN;
        return this;
    }

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
        if (t == null
                || v == null
                || t instanceof Number && v instanceof Number
                || t.getClass() == v.getClass()) {
            return predicate.test(t, v);
        } else {
            if (!typeCheckLogged) {
                // TODO: rewrite the message to be python friendly
                LOG.error("Cannot compare values of different types: {} and {}. RuleSet: {}. RuleName: {}. Condition: {}",
                          t.getClass(), v.getClass(), conditionContext.getRuleSet(), conditionContext.getRuleName(), conditionContext.getConditionExpression());
                typeCheckLogged = true; // Log only once per constraint
            }
            return false; // Different types are never equal
        }
    }

    public RulebookConstraintOperator inverse() {
        switch (this.type) {
            case GREATER_THAN:
                this.type = RulebookConstraintOperatorType.LESS_THAN;
                return this;
            case GREATER_OR_EQUAL:
                this.type =  RulebookConstraintOperatorType.LESS_OR_EQUAL;
                return this;
            case LESS_THAN:
                this.type = RulebookConstraintOperatorType.GREATER_THAN;
                return this;
            case LESS_OR_EQUAL:
                this.type = RulebookConstraintOperatorType.GREATER_OR_EQUAL;
                return this;
            default:
                return this;
        }
    }

    public boolean isComparison() {
        return isAscending() || isDescending();
    }

    public boolean isAscending() {
        return this.type == RulebookConstraintOperatorType.GREATER_THAN || this.type == RulebookConstraintOperatorType.GREATER_OR_EQUAL;
    }

    public boolean isDescending() {
        return this.type == RulebookConstraintOperatorType.LESS_THAN || this.type == RulebookConstraintOperatorType.LESS_OR_EQUAL;
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
}
