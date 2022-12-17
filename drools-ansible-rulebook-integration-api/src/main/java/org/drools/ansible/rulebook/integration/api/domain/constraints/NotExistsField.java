package org.drools.ansible.rulebook.integration.api.domain.constraints;

import java.util.function.BiPredicate;

import org.drools.model.ConstraintOperator;
import org.drools.model.PrototypeFact;

public enum NotExistsField implements ConstraintOperator {

    INSTANCE;

    @Override
    public <T, V> BiPredicate<T, V> asPredicate() {
        return (t,v) -> !((PrototypeFact) t).has((String) v);
    }

    @Override
    public String toString() {
        return "NOT_EXISTS_FIELD";
    }
}
