package org.drools.ansible.rulebook.integration.api.domain.constraints;

import java.util.Collection;
import java.util.function.BiPredicate;

import org.drools.model.ConstraintOperator;

public enum ListContainsConstraint implements ConstraintOperator {

    INSTANCE;

    @Override
    public <T, V> BiPredicate<T, V> asPredicate() {
        return ListContainsConstraint::listContains;
    }

    static <T, V> boolean listContains(T t, V v) {
        if (t instanceof Collection) {
            return ((Collection) t).contains(v);
        }
        return t.equals(v);
    }

    @Override
    public String toString() {
        return "LIST_CONTAINS";
    }
}
