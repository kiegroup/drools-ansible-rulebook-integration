package org.drools.ansible.rulebook.integration.api.domain.conditions;

import org.drools.model.ConstraintOperator;
import org.drools.model.prototype.PrototypeExpression;

public class BetaExpressionCondition extends ExpressionCondition {

    private final String otherBinding;

    public BetaExpressionCondition(String patternBinding, PrototypeExpression left, ConstraintOperator operator, String otherBinding, PrototypeExpression right) {
        super(patternBinding, left, operator, right);
        this.otherBinding = otherBinding;
    }

    @Override
    public String otherBinding() {
        return otherBinding;
    }

    @Override
    public boolean beta() {
        return true;
    }

    @Override
    public String toString() {
        return left + " " + operator + " " + otherBinding + "." + right;
    }
}
