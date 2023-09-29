package org.drools.ansible.rulebook.integration.api.domain.constraints;

import java.util.Collection;
import java.util.function.BiPredicate;

import static org.drools.model.util.OperatorUtils.areEqual;

public enum ListContainsConstraint implements RulebookOperator {

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

    @Override
    public boolean canInverse() {
        return true;
    }

    @Override
    public RulebookOperator inverse() {
        return ItemInListConstraint.INSTANCE;
    }
}
