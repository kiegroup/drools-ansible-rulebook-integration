package org.drools.ansible.rulebook.integration.api.rulesmodel;

import org.drools.model.ConstraintOperator;
import org.drools.model.PrototypeDSL;
import org.drools.model.PrototypeExpression;
import org.drools.model.PrototypeVariable;
import org.drools.model.view.ViewItem;
import org.drools.ansible.rulebook.integration.api.domain.RuleGenerationContext;

public class BetaParsedCondition extends ParsedCondition {

    private final PrototypeVariable betaVariable;

    public BetaParsedCondition(PrototypeExpression left, ConstraintOperator operator, PrototypeVariable betaVariable, PrototypeExpression right) {
        super(left, operator, right);
        this.betaVariable = betaVariable;
    }

    @Override
    public ViewItem addConditionToPattern(RuleGenerationContext ruleContext, PrototypeDSL.PrototypePatternDef pattern) {
        pattern.expr(getLeft(), getOperator(), betaVariable, getRight());
        return pattern;
    }
}