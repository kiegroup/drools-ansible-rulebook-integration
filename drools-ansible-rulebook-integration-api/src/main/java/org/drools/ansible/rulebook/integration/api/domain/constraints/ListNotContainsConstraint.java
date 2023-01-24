package org.drools.ansible.rulebook.integration.api.domain.constraints;

import java.util.function.BiPredicate;

import org.drools.model.ConstraintOperator;

import static org.drools.ansible.rulebook.integration.api.domain.constraints.ListContainsConstraint.listContains;

public enum ListNotContainsConstraint implements ConstraintOperator {

    INSTANCE;

    public static final String EXPRESSION_NAME = "ListNotContainsItemExpression";

    @Override
    public <T, V> BiPredicate<T, V> asPredicate() {
        return (t,v) -> !listContains(t, v);
    }

    @Override
    public String toString() {
        return "LIST_NOT_CONTAINS";
    }
}
