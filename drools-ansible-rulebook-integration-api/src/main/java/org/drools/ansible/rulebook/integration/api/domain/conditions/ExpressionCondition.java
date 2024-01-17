package org.drools.ansible.rulebook.integration.api.domain.conditions;

import org.drools.ansible.rulebook.integration.api.rulesmodel.ParsedCondition;
import org.drools.model.ConstraintOperator;
import org.drools.model.prototype.PrototypeExpression;

public class ExpressionCondition extends SimpleCondition {
    protected final PrototypeExpression left;
    protected final ConstraintOperator operator;
    protected final PrototypeExpression right;

    public ExpressionCondition(PrototypeExpression left, ConstraintOperator operator, PrototypeExpression right) {
        this.left = left;
        this.operator = operator;
        this.right = right;
    }

    public ExpressionCondition(String patternBinding, PrototypeExpression left, ConstraintOperator operator, PrototypeExpression right) {
        this(left, operator, right);
        setPatternBinding(patternBinding);
    }

    @Override
    public ParsedCondition parse() {
        return new ParsedCondition(left, operator, right);
    }

    @Override
    public String toString() {
        return left + " " + operator + " " + right;
    }
}
