package org.drools.ansible.rulebook.integration.api.domain.constraints;

import java.util.function.BiPredicate;

import static org.drools.ansible.rulebook.integration.api.domain.constraints.ListContainsConstraint.listContains;

public enum ListNotContainsConstraint implements RulebookOperator {

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

    @Override
    public boolean canInverse() {
        return true;
    }

    @Override
    public RulebookOperator inverse() {
        return ItemNotInListConstraint.INSTANCE;
    }
}
