package org.drools.ansible.rulebook.integration.api.domain.constraints;

import java.util.Collection;
import java.util.function.BiPredicate;

import org.drools.model.ConstraintOperator;

import static org.drools.ansible.rulebook.integration.api.domain.constraints.Operators.areEqual;

public enum ListContainsConstraint implements ConstraintOperator {

    INSTANCE;

    public static final String EXPRESSION_NAME = "ListContainsItemExpression";

    @Override
    public <T, V> BiPredicate<T, V> asPredicate() {
        return ListContainsConstraint::listContains;
    }

    static <T, V> boolean listContains(T t, V v) {
        if (t instanceof Collection) {
            return ((Collection) t).stream().anyMatch(item -> areEqual(item, v));
        }
        return areEqual(t, v);
    }

    @Override
    public String toString() {
        return "LIST_CONTAINS";
    }
}
