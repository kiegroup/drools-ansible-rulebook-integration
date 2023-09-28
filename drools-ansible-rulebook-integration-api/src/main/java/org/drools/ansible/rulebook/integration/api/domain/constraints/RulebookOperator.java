package org.drools.ansible.rulebook.integration.api.domain.constraints;

import org.drools.model.ConstraintOperator;
import org.drools.model.Index;

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

    RulebookOperator EQUAL = new OperatorWrapper(Index.ConstraintType.EQUAL);
    RulebookOperator NOT_EQUAL = new OperatorWrapper(Index.ConstraintType.NOT_EQUAL);
    RulebookOperator GREATER_THAN = new OperatorWrapper(Index.ConstraintType.GREATER_THAN);
    RulebookOperator GREATER_OR_EQUAL = new OperatorWrapper(Index.ConstraintType.GREATER_OR_EQUAL);
    RulebookOperator LESS_THAN = new OperatorWrapper(Index.ConstraintType.LESS_THAN);
    RulebookOperator LESS_OR_EQUAL = new OperatorWrapper(Index.ConstraintType.LESS_OR_EQUAL);

    class OperatorWrapper implements RulebookOperator {
        private final Index.ConstraintType delegate;

        public OperatorWrapper(Index.ConstraintType delegate) {
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
    }
}
