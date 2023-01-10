package org.drools.ansible.rulebook.integration.api.domain.constraints;

import java.util.function.BiPredicate;

import org.drools.model.ConstraintOperator;

public class NegationOperator implements ConstraintOperator {

    private final ConstraintOperator toBeNegated;

    public NegationOperator(ConstraintOperator toBeNegated) {
        this.toBeNegated = toBeNegated;
    }

    @Override
    public <T, V> BiPredicate<T, V> asPredicate() {
        return (t, v) -> !toBeNegated.asPredicate().test(t, v);
    }

    @Override
    public String toString() {
        return "NOT( " + toBeNegated + " )";
    }
}
