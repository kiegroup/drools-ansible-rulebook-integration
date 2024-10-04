package org.drools.ansible.rulebook.integration.api.domain.constraints;

import java.util.function.BiPredicate;

import org.drools.model.ConstraintOperator;

import static org.drools.ansible.rulebook.integration.api.domain.constraints.RulebookConstraintOperator.isCompatibleType;

public class NegationOperator implements ConstraintOperator {

    private final ConstraintOperator toBeNegated;

    public NegationOperator(ConstraintOperator toBeNegated) {
        this.toBeNegated = toBeNegated;
    }

    @Override
    public <T, V> BiPredicate<T, V> asPredicate() {
        if (toBeNegated instanceof RulebookConstraintOperator) {
            return (t, v) -> {
                // if not compatible type, return false regardless of the result
                // but let the operator be executed anyway so that the error message specific to the constraint is logged
                boolean result = !toBeNegated.asPredicate().test(t, v);
                return isCompatibleType(t, v) ? result : false;
            };
        }
        return (t, v) -> !toBeNegated.asPredicate().test(t, v);
    }

    @Override
    public String toString() {
        return "NOT( " + toBeNegated + " )";
    }
}
