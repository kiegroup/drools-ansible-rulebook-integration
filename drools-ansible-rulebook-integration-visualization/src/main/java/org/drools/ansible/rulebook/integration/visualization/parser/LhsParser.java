package org.drools.ansible.rulebook.integration.visualization.parser;

import java.util.List;

import org.drools.ansible.rulebook.integration.api.domain.RuleGenerationContext;
import org.drools.ansible.rulebook.integration.api.domain.conditions.AstCondition;
import org.drools.ansible.rulebook.integration.api.domain.conditions.Condition;
import org.drools.ansible.rulebook.integration.api.domain.conditions.MapCondition;
import org.drools.ansible.rulebook.integration.api.rulesmodel.ParsedCondition;
import org.drools.base.facttemplates.FactTemplateObjectType;
import org.drools.impact.analysis.model.left.Constraint;
import org.drools.impact.analysis.model.left.LeftHandSide;
import org.drools.impact.analysis.model.left.Pattern;
import org.drools.model.ConstraintOperator;
import org.drools.model.Index;
import org.drools.model.PrototypeExpression;

public class LhsParser {

    private LhsParser() {
        // intentionally left blank
    }

    public static void parse(Condition condition, LeftHandSide lhs) {
        Pattern pattern = new Pattern(FactTemplateObjectType.class, true);
        parseConditions(condition, pattern);
        lhs.addPattern(pattern);
    }

    private static void parseConditions(Condition condition, Pattern pattern) {
        if (condition instanceof MapCondition mapCondition) {
            // Firstly, process the raw condition
            RuleGenerationContext ruleContext = new RuleGenerationContext();
            Condition astCondition = MapCondition.map2Ast(ruleContext, mapCondition, null);
            parseConditions(astCondition, pattern);
        } else if (condition instanceof AstCondition.MultipleConditions multipleConditions) {
            // All and Any conditions
            List<Condition> conditions = multipleConditions.getConditions();
            conditions.forEach(c -> parseConditions(c, pattern));
        } else if (condition instanceof AstCondition.SingleCondition singleCondition) {
            // Single condition turns into a constraint
            ParsedCondition parsedCondition = singleCondition.getParsedCondition();
            PrototypeExpression left = parsedCondition.getLeft();
            ConstraintOperator operator = parsedCondition.getOperator();
            PrototypeExpression right = parsedCondition.getRight();
            Constraint constraint = createConstraint(operator, left, right);
            pattern.addConstraint(constraint);
            pattern.addReactOn(constraint.getProperty());
        } else {
            throw new UnsupportedOperationException("Unsupported condition type: " + condition.getClass().getName());
        }
    }

    private static Constraint createConstraint(ConstraintOperator operator, PrototypeExpression left, PrototypeExpression right) {
        // quick implementation assuming that the right is always a fixed value
        return new Constraint(convertConstraintOperator(operator), left.getIndexingKey().get(), ((PrototypeExpression.FixedValue) right).getValue());
    }

    private static Constraint.Type convertConstraintOperator(ConstraintOperator operator) {
        if (operator instanceof Index.ConstraintType constraintType) {
            switch (constraintType) {
                case EQUAL:
                    return Constraint.Type.EQUAL;
                case NOT_EQUAL:
                    return Constraint.Type.NOT_EQUAL;
                case GREATER_THAN:
                    return Constraint.Type.GREATER_THAN;
                case GREATER_OR_EQUAL:
                    return Constraint.Type.GREATER_OR_EQUAL;
                case LESS_OR_EQUAL:
                    return Constraint.Type.LESS_OR_EQUAL;
                case LESS_THAN:
                    return Constraint.Type.LESS_THAN;
                default:
                    return Constraint.Type.UNKNOWN;
            }
        }
        return Constraint.Type.UNKNOWN;
    }
}
