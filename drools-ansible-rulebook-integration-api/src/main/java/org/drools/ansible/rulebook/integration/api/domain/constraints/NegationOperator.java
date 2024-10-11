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
        if (toBeNegated instanceof RulebookConstraintOperator rulebookConstraintOperator) {
            return (t, v) -> {
                // if not compatible type, return false. Use the operator to log the type check error
                if (!isCompatibleType(t, v)) {
                    rulebookConstraintOperator.logTypeCheck(t, v);
                    return false;
                }
                return !toBeNegated.asPredicate().test(t, v);
            };
        }
        return (t, v) -> !toBeNegated.asPredicate().test(t, v);
    }

    @Override
    public String toString() {
        return "NOT( " + toBeNegated + " )";
    }
}
