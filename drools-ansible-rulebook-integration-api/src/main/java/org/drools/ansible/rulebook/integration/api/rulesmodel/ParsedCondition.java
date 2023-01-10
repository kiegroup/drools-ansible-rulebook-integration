package org.drools.ansible.rulebook.integration.api.rulesmodel;

import org.drools.ansible.rulebook.integration.api.domain.constraints.NegationOperator;
import org.drools.model.ConstraintOperator;
import org.drools.model.PrototypeDSL;
import org.drools.model.PrototypeExpression;
import org.drools.model.PrototypeVariable;
import org.drools.model.view.CombinedExprViewItem;
import org.drools.model.view.ViewItem;
import org.drools.ansible.rulebook.integration.api.domain.RuleGenerationContext;

import static org.drools.model.DSL.not;
import static org.drools.model.PrototypeExpression.fixedValue;
import static org.drools.model.PrototypeExpression.prototypeField;

public class ParsedCondition {

    private final PrototypeExpression left;
    private final ConstraintOperator operator;
    private final PrototypeExpression right;

    private boolean notPattern = false;

    private boolean implicitPattern = false;

    private boolean negated = false;

    public ParsedCondition(String left, ConstraintOperator operator, Object right) {
        this(prototypeField(left), operator, fixedValue(right));
    }

    public ParsedCondition(PrototypeExpression left, ConstraintOperator operator, PrototypeExpression right) {
        this.left = left;
        this.operator = operator;
        this.right = right;
    }

    public PrototypeExpression getLeft() {
        return left;
    }

    public ConstraintOperator getOperator() {
        return negated ? new NegationOperator(operator) : operator;
    }

    public PrototypeExpression getRight() {
        return right;
    }

    public ParsedCondition withNotPattern(boolean notPattern) {
        this.notPattern = notPattern;
        return this;
    }

    public ParsedCondition withImplicitPattern(boolean implicitPattern) {
        this.implicitPattern = implicitPattern;
        return this;
    }

    public ParsedCondition negate() {
        this.negated = true;
        return this;
    }

    public ViewItem addConditionToPattern(RuleGenerationContext ruleContext, PrototypeDSL.PrototypePatternDef pattern) {
        if (implicitPattern) {
            PrototypeDSL.PrototypePatternDef first = ruleContext.getOrCreatePattern(ruleContext.generateBinding(), PrototypeFactory.DEFAULT_PROTOTYPE_NAME);
            pattern.expr(getLeft(), getOperator(), (PrototypeVariable) first.getFirstVariable(), getRight());
            return new CombinedExprViewItem(org.drools.model.Condition.Type.AND, new ViewItem[] { first, pattern });
        }

        pattern.expr(getLeft(), getOperator(), getRight());
        return notPattern ? not(pattern) : pattern;
    }

    @Override
    public String toString() {
        return getLeft() + " " + getOperator() + " " + getRight();
    }
}