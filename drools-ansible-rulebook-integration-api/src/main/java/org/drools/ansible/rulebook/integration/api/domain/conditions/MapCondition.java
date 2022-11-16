package org.drools.ansible.rulebook.integration.api.domain.conditions;

import java.util.List;
import java.util.Map;

import org.drools.ansible.rulebook.integration.api.RuleConfigurationOption;
import org.drools.model.Index;
import org.drools.model.PrototypeDSL.PrototypePatternDef;
import org.drools.model.PrototypeExpression;
import org.drools.model.PrototypeVariable;
import org.drools.model.view.ViewItem;
import org.drools.ansible.rulebook.integration.api.domain.RuleGenerationContext;
import org.drools.ansible.rulebook.integration.api.rulesmodel.BetaParsedCondition;
import org.drools.ansible.rulebook.integration.api.rulesmodel.ParsedCondition;

import static org.drools.model.Index.ConstraintType.EXISTS_PROTOTYPE_FIELD;
import static org.drools.model.PrototypeDSL.fieldName2PrototypeExpression;
import static org.drools.model.PrototypeExpression.fixedValue;

public class MapCondition implements Condition {

    private Map<?,?> map;

    private String patternBinding;

    public MapCondition() { } // used for serialization

    public MapCondition(Map<?,?> map) {
        this.map = map;
    }

    public Map<?,?> getMap() {
        return map;
    }

    public void setMap(Map<?,?> map) {
        this.map = map;
    }

    private String getPatternBinding(RuleGenerationContext ruleContext) {
        if (patternBinding == null) {
            patternBinding = ruleContext.generateBinding();
        }
        return patternBinding;
    }

    public void setPatternBinding(String patternBinding) {
        this.patternBinding = patternBinding;
    }

    @Override
    public ViewItem toPattern(RuleGenerationContext ruleContext) {
        return map2Ast(ruleContext, parseConditionAttributes(ruleContext, this), null).toPattern(ruleContext);
    }

    private static MapCondition parseConditionAttributes(RuleGenerationContext ruleContext, MapCondition condition) {
        Object onceWithin = condition.getMap().remove("once_within");
        if (onceWithin != null) {
            Object uniqueAttributes = condition.getMap().remove("unique_attributes");
            if (uniqueAttributes == null) {
                throw new IllegalArgumentException("once_within also requires unique_attributes");
            }
            ruleContext.setTimeConstraint(OnceWithinDefinition.parseOnceWithin((String) onceWithin, (List<String>) uniqueAttributes));
        }

        Object timeWindow = condition.getMap().remove("time_window");
        if (timeWindow != null) {
            ruleContext.setTimeConstraint(TimeWindowDefinition.parseTimeWindow((String) timeWindow));
        }

        return condition;
    }

    private static Condition map2Ast(RuleGenerationContext ruleContext, MapCondition condition, AstCondition.MultipleConditions parent) {
        assert(condition.getMap().size() == 1);
        Map.Entry entry = condition.getMap().entrySet().iterator().next();
        String expressionName = (String) entry.getKey();
        switch (expressionName) {
            case "OrExpression":
                String orBinding = condition.getPatternBinding(ruleContext);
                Map.Entry lhsEntry = ((Map<?,?>) ((Map) entry.getValue()).get("lhs")).entrySet().iterator().next();
                Map.Entry rhsEntry = ((Map<?,?>) ((Map) entry.getValue()).get("rhs")).entrySet().iterator().next();
                return new AstCondition.OrCondition()
                        .withLhs(new AstCondition.SingleCondition(parent, condition.parseSingle(ruleContext, lhsEntry)).withPatternBinding(ruleContext, orBinding))
                        .withRhs(new AstCondition.SingleCondition(parent, condition.parseSingle(ruleContext, rhsEntry)).withPatternBinding(ruleContext, orBinding));
            case "AndExpression":
                String andBinding = condition.getPatternBinding(ruleContext);
                MapCondition lhs = new MapCondition((Map) ((Map) entry.getValue()).get("lhs"));
                lhs.setPatternBinding(andBinding);
                MapCondition rhs = new MapCondition((Map) ((Map) entry.getValue()).get("rhs"));
                rhs.setPatternBinding(andBinding);
                return new AstCondition.AndCondition()
                        .withLhs(map2Ast(ruleContext, lhs, null))
                        .withRhs(map2Ast(ruleContext, rhs, null));
            case "AnyCondition":
            case "AllCondition":
                AstCondition.MultipleConditions conditions = expressionName.equals("AnyCondition") ?
                        new AstCondition.AnyCondition(ruleContext) : new AstCondition.AllCondition(ruleContext);
                for (Map subC : (List<Map>) entry.getValue()) {
                    conditions.addCondition(map2Ast(ruleContext, new MapCondition(subC), conditions));
                }
                return conditions;
        }

        return new AstCondition.SingleCondition(parent, condition.parseSingle(ruleContext, entry))
                .withPatternBinding(ruleContext, condition.getPatternBinding(ruleContext));
    }

    private ParsedCondition parseSingle(RuleGenerationContext ruleContext, Map.Entry entry) {
        String expressionName = (String) entry.getKey();
        Map<?,?> expression = (Map<?,?>) entry.getValue();

        if (expressionName.equals("AssignmentExpression")) {
            Map<?,?> assignment = (Map<?,?>) expression.get("lhs");
            assert(assignment.size() == 1);
            this.patternBinding = (String) assignment.values().iterator().next();

            Map<?,?> assigned = (Map<?,?>) expression.get("rhs");
            assert(assigned.size() == 1);
            return parseSingle(ruleContext, assigned.entrySet().iterator().next());
        }

        Index.ConstraintType operator = decodeOperation(expressionName);

        if (operator == EXISTS_PROTOTYPE_FIELD) {
            return new ParsedCondition(map2Expr(ruleContext, expression).prototypeExpression, operator, fixedValue(true)).withNotPattern(expressionName.equals("IsNotDefinedExpression"));
        }

        ConditionExpression left = map2Expr(ruleContext, expression.get("lhs"));
        ConditionExpression right = map2Expr(ruleContext, expression.get("rhs"));
        return right.isBeta() ?
                new BetaParsedCondition(left.prototypeExpression, operator, right.betaVariable, right.prototypeExpression) :
                new ParsedCondition(left.prototypeExpression, operator, right.prototypeExpression).withImplicitPattern(hasImplicitPattern(ruleContext, left, right));
    }

    private boolean hasImplicitPattern(RuleGenerationContext ruleContext, ConditionExpression left, ConditionExpression right) {
        boolean hasImplicitPattern = left.field && right.field && !left.prototypeName.equals(right.prototypeName) && !ruleContext.isExistingBoundVariable(right.prototypeName);
        if (hasImplicitPattern && !ruleContext.hasOption(RuleConfigurationOption.ALLOW_IMPLICIT_JOINS)) {
            throw new UnsupportedOperationException("Cannot have an implicit pattern without using ALLOW_IMPLICIT_JOINS option");
        }
        return hasImplicitPattern;
    }

    private static ConditionExpression map2Expr(RuleGenerationContext ruleContext, Object expr) {
        if (expr instanceof String) {
            return createFieldExpression(ruleContext, (String)expr);
        }

        Map<?,?> exprMap = (Map) expr;
        assert(exprMap.size() == 1);
        Map.Entry entry = exprMap.entrySet().iterator().next();
        String key = (String) entry.getKey();
        Object value = entry.getValue();

        switch (key) {
            case "Integer":
            case "String":
            case "Boolean":
                return new ConditionExpression(fixedValue(value));
        }

        if (value instanceof String) {
            String fieldName = ignoreKey(key) ? (String) value : key + "." + value;
            return createFieldExpression(ruleContext, fieldName);
        }

        if (value instanceof Map) {
            Map<?,?> expression = (Map<?,?>) value;
            return map2Expr(ruleContext, expression.get("lhs")).composeWith( decodeBinaryOperator(key), map2Expr(ruleContext, expression.get("rhs")));
        }

        throw new UnsupportedOperationException("Invalid expression: " + expr);
    }

    private static ConditionExpression createFieldExpression(RuleGenerationContext ruleContext, String fieldName) {
        int dotPos = fieldName.indexOf('.');
        String prototypeName = fieldName;
        PrototypeVariable betaVariable = null;
        if (dotPos > 0) {
            prototypeName = fieldName.substring(0, dotPos);
            PrototypePatternDef boundPattern = ruleContext.getBoundPattern(prototypeName);
            if ( boundPattern != null ) {
                fieldName = fieldName.substring(dotPos+1);
                betaVariable = (PrototypeVariable) boundPattern.getFirstVariable();
            }
        }
        return new ConditionExpression(fieldName2PrototypeExpression(fieldName), true, prototypeName, betaVariable);
    }

    private static boolean ignoreKey(String key) {
        return key.equalsIgnoreCase("fact") || key.equalsIgnoreCase("facts") || key.equalsIgnoreCase("event") || key.equalsIgnoreCase("events");
    }

    private static class ConditionExpression {
        private final PrototypeExpression prototypeExpression;
        private final boolean field;
        private final String prototypeName;
        private final PrototypeVariable betaVariable;

        private ConditionExpression(PrototypeExpression prototypeExpression) {
            this(prototypeExpression, false, null, null);
        }

        private ConditionExpression(PrototypeExpression prototypeExpression, boolean field, String prototypeName, PrototypeVariable betaVariable) {
            this.prototypeExpression = prototypeExpression;
            this.field = field;
            this.prototypeName = prototypeName;
            this.betaVariable = betaVariable;
        }

        public ConditionExpression composeWith(PrototypeExpression.BinaryOperation.Operator decodeBinaryOperator, ConditionExpression rhs) {
            PrototypeExpression composed = prototypeExpression.composeWith(decodeBinaryOperator, rhs.prototypeExpression);
            if (field) {
                return new ConditionExpression(composed, true, prototypeName, betaVariable);
            }
            if (rhs.field) {
                return new ConditionExpression(composed, true, prototypeName, betaVariable);
            }
            return new ConditionExpression(composed);
        }

        public boolean isBeta() {
            return betaVariable != null;
        }

        @Override
        public String toString() {
            return "ConditionExpression{" +
                    "prototypeExpression=" + prototypeExpression +
                    ", field=" + field +
                    ", prototypeName='" + prototypeName + '\'' +
                    '}';
        }
    }

    private static Index.ConstraintType decodeOperation(String expressionName) {
        switch (expressionName) {
            case "EqualsExpression":
                return Index.ConstraintType.EQUAL;
            case "NotEqualsExpression":
                return Index.ConstraintType.NOT_EQUAL;
            case "GreaterThanExpression":
                return Index.ConstraintType.GREATER_THAN;
            case "GreaterThanOrEqualToExpression":
                return Index.ConstraintType.GREATER_OR_EQUAL;
            case "LessThanExpression":
                return Index.ConstraintType.LESS_THAN;
            case "LessThanOrEqualToExpression":
                return Index.ConstraintType.LESS_OR_EQUAL;
            case "IsDefinedExpression":
            case "IsNotDefinedExpression":
                return EXISTS_PROTOTYPE_FIELD;
        }
        throw new UnsupportedOperationException("Unrecognized operation type: " + expressionName);
    }

    private static PrototypeExpression.BinaryOperation.Operator decodeBinaryOperator(String operator) {
        switch (operator) {
            case "AdditionExpression":
                return PrototypeExpression.BinaryOperation.Operator.ADD;
            case "SubtractionExpression":
                return PrototypeExpression.BinaryOperation.Operator.SUB;
            case "MultiplicationExpression":
                return PrototypeExpression.BinaryOperation.Operator.MUL;
            case "DivisionExpression":
                return PrototypeExpression.BinaryOperation.Operator.DIV;
        }
        throw new UnsupportedOperationException("Unrecognized binary operator " + operator);
    }

    @Override
    public String toString() {
        return "MapCondition{" +
                "map=" + map +
                '}';
    }
}
