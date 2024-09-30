package org.drools.ansible.rulebook.integration.api.domain.constraints;

import java.util.Map;
import java.util.function.BiPredicate;

import org.drools.ansible.rulebook.integration.api.domain.RuleGenerationContext;
import org.drools.model.ConstraintOperator;
import org.drools.model.Index;

public interface RulebookOperator extends ConstraintOperator {

    default boolean canInverse() {
        return false;
    }

    default RulebookOperator inverse() {
        throw new UnsupportedOperationException("Operator " + getClass().getSimpleName() + " cannot be inverted");
    }

    default ConstraintOperator asConstraintOperator() {
        return this;
    }

    static RulebookOperator newEqual() {
        return new RulebookConstraintOperator(Index.ConstraintType.EQUAL);
    }

    static RulebookOperator newNotEqual() {
        return new RulebookConstraintOperator(Index.ConstraintType.NOT_EQUAL);
    }

    static RulebookOperator newGreaterThan() {
        return new RulebookConstraintOperator(Index.ConstraintType.GREATER_THAN);
    }

    static RulebookOperator newGreaterOrEqual() {
        return new RulebookConstraintOperator(Index.ConstraintType.GREATER_OR_EQUAL);
    }

    static RulebookOperator newLessThan() {
        return new RulebookConstraintOperator(Index.ConstraintType.LESS_THAN);
    }

    static RulebookOperator newLessOrEqual() {
        return new RulebookConstraintOperator(Index.ConstraintType.LESS_OR_EQUAL);
    }

    default void setConditionContext(RuleGenerationContext ruleContext, Map<?, ?> expression) {
        if (this instanceof RulebookConstraintOperator rulebookConstraintOperator) {
            rulebookConstraintOperator.setConditionContext(ruleContext, expression);
        } else {
            // do nothing
        }
    }
}
