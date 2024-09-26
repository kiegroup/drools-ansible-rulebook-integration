package org.drools.ansible.rulebook.integration.api.domain.constraints;

import org.drools.ansible.rulebook.integration.api.domain.RuleGenerationContext;
import org.drools.model.ConstraintOperator;

import java.util.Map;
import java.util.function.BiPredicate;

public interface RulebookOperator extends ConstraintOperator {

    default boolean canInverse() {
        return false;
    }

    default RulebookOperator inverse() {
        throw new UnsupportedOperationException("Operator " + getClass().getSimpleName() + " cannot be inverted");
    }

    default ConstraintOperator asConstraintOperator() {
        return this;
    }

    static RulebookOperator newEqual() {
        return new OperatorWrapper(new RulebookConstraintOperator(RulebookConstraintOperator.RulebookConstraintOperatorType.EQUAL));
    }

    static RulebookOperator newNotEqual() {
        return new OperatorWrapper(new RulebookConstraintOperator(RulebookConstraintOperator.RulebookConstraintOperatorType.NOT_EQUAL));
    }

    static RulebookOperator newGreaterThan() {
        return new OperatorWrapper(new RulebookConstraintOperator(RulebookConstraintOperator.RulebookConstraintOperatorType.GREATER_THAN));
    }

    static RulebookOperator newGreaterOrEqual() {
        return new OperatorWrapper(new RulebookConstraintOperator(RulebookConstraintOperator.RulebookConstraintOperatorType.GREATER_OR_EQUAL));
    }

    static RulebookOperator newLessThan() {
        return new OperatorWrapper(new RulebookConstraintOperator(RulebookConstraintOperator.RulebookConstraintOperatorType.LESS_THAN));
    }

    static RulebookOperator newLessOrEqual() {
        return new OperatorWrapper(new RulebookConstraintOperator(RulebookConstraintOperator.RulebookConstraintOperatorType.LESS_OR_EQUAL));
    }

    default void setConditionContext(RuleGenerationContext ruleContext, Map<?, ?> expression) {
        if (this instanceof OperatorWrapper operatorWrapper) {
            operatorWrapper.setConditionContext(ruleContext, expression);
        } else {
            // do nothing
            return;
        }
    }

    class OperatorWrapper implements RulebookOperator {
        private final RulebookConstraintOperator delegate;

        public OperatorWrapper(RulebookConstraintOperator delegate) {
            this.delegate = delegate;
        }

        @Override
        public <T, V> BiPredicate<T, V> asPredicate() {
            return delegate.asPredicate();
        }

        @Override
        public boolean canInverse() {
            return delegate.canInverse();
        }

        @Override
        public RulebookOperator inverse() {
            return new OperatorWrapper(delegate.inverse());
        }

        @Override
        public ConstraintOperator asConstraintOperator() {
            return delegate;
        }

        public void setConditionContext(RuleGenerationContext ruleContext, Map<?, ?> expression) {
            delegate.setConditionContext(ruleContext, expression);
        }
    }
}
