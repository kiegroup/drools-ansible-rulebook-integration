package org.drools.ansible.rulebook.integration.api.domain.constraints;

import org.drools.model.ConstraintOperator;

import java.util.function.BiPredicate;

@Deprecated() // TODO seems no longer in-use?
public enum NotExistsField implements ConstraintOperator {

    INSTANCE;

    @Override
    public <T, V> BiPredicate<T, V> asPredicate() {
        throw new UnsupportedOperationException("deprecated and should be no longer in use");
    }

    @Override
    public String toString() {
        return "NOT_EXISTS_FIELD";
    }
}
