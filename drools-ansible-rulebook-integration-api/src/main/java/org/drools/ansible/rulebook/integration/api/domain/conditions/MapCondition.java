package org.drools.ansible.rulebook.integration.api.domain.conditions;

import java.util.List;
import java.util.Map;

import org.drools.ansible.rulebook.integration.api.domain.RuleGenerationContext;
import org.drools.ansible.rulebook.integration.api.domain.constraints.ConditionFactory;
import org.drools.ansible.rulebook.integration.api.domain.constraints.ExistsField;
import org.drools.ansible.rulebook.integration.api.domain.constraints.ItemInListConstraint;
import org.drools.ansible.rulebook.integration.api.domain.constraints.ItemNotInListConstraint;
import org.drools.ansible.rulebook.integration.api.domain.constraints.ListContainsConstraint;
import org.drools.ansible.rulebook.integration.api.domain.constraints.ListNotContainsConstraint;
import org.drools.ansible.rulebook.integration.api.domain.constraints.SearchMatchesConstraint;
import org.drools.ansible.rulebook.integration.api.domain.constraints.SelectAttrConstraint;
import org.drools.ansible.rulebook.integration.api.domain.constraints.SelectConstraint;
import org.drools.ansible.rulebook.integration.api.domain.temporal.OnceAfterDefinition;
import org.drools.ansible.rulebook.integration.api.domain.temporal.OnceWithinDefinition;
import org.drools.ansible.rulebook.integration.api.domain.temporal.TimeConstraint;
import org.drools.ansible.rulebook.integration.api.domain.temporal.TimeWindowDefinition;
import org.drools.ansible.rulebook.integration.api.domain.temporal.TimedOutDefinition;
import org.drools.ansible.rulebook.integration.api.rulesmodel.BetaParsedCondition;
import org.drools.ansible.rulebook.integration.api.rulesmodel.ParsedCondition;
import org.drools.model.ConstraintOperator;
import org.drools.model.Index;
import org.drools.model.view.ViewItem;

import static org.drools.ansible.rulebook.integration.api.domain.conditions.ConditionExpression.map2Expr;
import static org.drools.ansible.rulebook.integration.api.domain.conditions.ConditionExpression.mapEntry2Expr;
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

    private MapCondition withPatternBinding(String patternBinding) {
        this.patternBinding = patternBinding;
        return this;
    }

    @Override
    public ViewItem toPattern(RuleGenerationContext ruleContext) {
        return map2Ast(ruleContext, parseConditionAttributes(ruleContext, this), null).toPattern(ruleContext);
    }

    private static MapCondition parseConditionAttributes(RuleGenerationContext ruleContext, MapCondition condition) {
        parseThrottle(ruleContext, condition);
        parseTimeOut(ruleContext, condition);
        return condition;
    }

    private static void parseThrottle(RuleGenerationContext ruleContext, MapCondition condition) {
        Map throttle = (Map) condition.getMap().remove("throttle");
        if (throttle != null) {
            List<String> groupByAttributes = (List<String>) throttle.get(TimeConstraint.GROUP_BY_ATTRIBUTES);
            String onceWithin = (String) throttle.get(OnceWithinDefinition.KEYWORD);
            if (onceWithin != null) {
                ruleContext.setTimeConstraint(OnceWithinDefinition.parseOnceWithin(onceWithin, groupByAttributes));
                return;
            }
            String onceAfter = (String) throttle.get(OnceAfterDefinition.KEYWORD);
            if (onceAfter != null) {
                ruleContext.setTimeConstraint(OnceAfterDefinition.parseOnceAfter(onceAfter, groupByAttributes));
                return;
            }
            throw new IllegalArgumentException("Invalid throttle definition");
        }
    }

    private static void parseTimeOut(RuleGenerationContext ruleContext, MapCondition condition) {
        String timeOut = (String) condition.getMap().remove("timeout");
        if (timeOut != null) {
            if (condition.getMap().containsKey("NotAllCondition")) {
                ruleContext.setTimeConstraint(TimedOutDefinition.parseTimedOut(timeOut));
            } else {
                ruleContext.setTimeConstraint(TimeWindowDefinition.parseTimeWindow(timeOut));
            }
        }
    }

    private static Condition map2Ast(RuleGenerationContext ruleContext, MapCondition condition, AstCondition.MultipleConditions parent) {
        assert(condition.getMap().size() == 1);
        Map.Entry entry = condition.getMap().entrySet().iterator().next();
        String expressionName = (String) entry.getKey();

        switch (expressionName) {
            case "NotAllCondition":
                if (ruleContext.getTimeConstraint().filter( tc -> tc instanceof TimedOutDefinition ).isEmpty()) {
                    throw new IllegalArgumentException("NotAllCondition requires a timeout");
                }
            case "AnyCondition":
            case "AllCondition":
                AstCondition.MultipleConditions conditions = expressionName.equals("AnyCondition") ?
                        new AstCondition.AnyCondition(ruleContext) : new AstCondition.AllCondition(ruleContext);
                List<Map> innerMaps = (List<Map>) entry.getValue();
                ruleContext.setMultiplePatterns(innerMaps.size() > 1);
                for (Map subC : innerMaps) {
                    conditions.addCondition(map2Ast(ruleContext, new MapCondition(subC), conditions));
                }
                ruleContext.setMultiplePatterns(false);
                return conditions;
        }

        return pattern2Ast(ruleContext, condition.withPatternBinding( condition.getPatternBinding(ruleContext) ), parent);
    }

    private static AstCondition.PatternCondition pattern2Ast(RuleGenerationContext ruleContext, MapCondition condition, AstCondition.MultipleConditions parent) {
        assert(condition.getMap().size() == 1);
        Map.Entry entry = condition.getMap().entrySet().iterator().next();
        String expressionName = (String) entry.getKey();

        switch (expressionName) {
            case "AssignmentExpression": {
                Map<?, ?> expression = (Map<?, ?>) entry.getValue();

                Map<?, ?> assignment = (Map<?, ?>) expression.get("lhs");
                assert (assignment.size() == 1);
                String binding = (String) assignment.values().iterator().next();

                Map<?, ?> assigned = (Map<?, ?>) expression.get("rhs");
                assert (assigned.size() == 1);
                return pattern2Ast(ruleContext, new MapCondition(assigned).withPatternBinding(binding), parent);
            }

            case "NegateExpression": {
                String binding = condition.getPatternBinding(ruleContext);
                return pattern2Ast(ruleContext, new MapCondition((Map) entry.getValue()).withPatternBinding(binding), parent).negate(ruleContext);
            }

            case "AndExpression":
            case "OrExpression": {
                String binding = condition.getPatternBinding(ruleContext);
                MapCondition lhs = new MapCondition((Map) ((Map) entry.getValue()).get("lhs")).withPatternBinding(binding);
                MapCondition rhs = new MapCondition((Map) ((Map) entry.getValue()).get("rhs")).withPatternBinding(binding);

                AstCondition.CombinedPatternCondition combinedPatternCondition = expressionName.equals("OrExpression") ?
                        new AstCondition.OrCondition(binding) :
                        new AstCondition.AndCondition(binding);
                return combinedPatternCondition
                        .withLhs(pattern2Ast(ruleContext, lhs, null))
                        .withRhs(pattern2Ast(ruleContext, rhs, null));
            }
        }

        return new AstCondition.SingleCondition(parent, condition.parseSingle(ruleContext, entry))
                .withPatternBinding(ruleContext, condition.getPatternBinding(ruleContext));
    }

    private ParsedCondition parseSingle(RuleGenerationContext ruleContext, Map.Entry entry) {
        String expressionName = (String) entry.getKey();
        if (expressionName.equals("Boolean")) {
            // "Boolean":true is the always true constraint
            return new ParsedCondition(fixedValue(true), Index.ConstraintType.EQUAL, fixedValue(entry.getValue()));
        }
        if (entry.getValue() instanceof String) {
            // the field name alone is the shortcut for fieldName == true
            return new ParsedCondition(mapEntry2Expr(ruleContext, entry).getPrototypeExpression(), Index.ConstraintType.EQUAL, fixedValue(true));
        }

        return parseExpression(ruleContext, expressionName, (Map<?,?>) entry.getValue());
    }

    private ParsedCondition parseExpression(RuleGenerationContext ruleContext, String expressionName, Map<?, ?> expression) {
        ConstraintOperator operator = decodeOperation(expressionName);

        if (operator instanceof ConditionFactory) {
            return ((ConditionFactory) operator).createParsedCondition(ruleContext, expressionName, expression);
        }

        ConditionExpression left = map2Expr(ruleContext, expression.get("lhs"));
        ConditionExpression right = map2Expr(ruleContext, expression.get("rhs"));
        return right.isBeta() ?
                new BetaParsedCondition(left.getPrototypeExpression(), operator, right.getBetaVariable(), right.getPrototypeExpression()) :
                new ParsedCondition(left.getPrototypeExpression(), operator, right.getPrototypeExpression());
    }

    private static ConstraintOperator decodeOperation(String expressionName) {
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
            case ExistsField.EXPRESSION_NAME:
            case ExistsField.NEGATED_EXPRESSION_NAME:
                return ExistsField.INSTANCE;
            case ListContainsConstraint.EXPRESSION_NAME:
                return ListContainsConstraint.INSTANCE;
            case ListNotContainsConstraint.EXPRESSION_NAME:
                return ListNotContainsConstraint.INSTANCE;
            case ItemInListConstraint.EXPRESSION_NAME:
                return ItemInListConstraint.INSTANCE;
            case ItemNotInListConstraint.EXPRESSION_NAME:
                return ItemNotInListConstraint.INSTANCE;
            case SearchMatchesConstraint.EXPRESSION_NAME:
            case SearchMatchesConstraint.NEGATED_EXPRESSION_NAME:
                return SearchMatchesConstraint.INSTANCE;
            case SelectConstraint.EXPRESSION_NAME:
            case SelectConstraint.NEGATED_EXPRESSION_NAME:
                return SelectConstraint.INSTANCE;
            case SelectAttrConstraint.EXPRESSION_NAME:
            case SelectAttrConstraint.NEGATED_EXPRESSION_NAME:
                return SelectAttrConstraint.INSTANCE;
        }
        throw new UnsupportedOperationException("Unrecognized operation type: " + expressionName);
    }

    @Override
    public String toString() {
        return "MapCondition{" +
                "map=" + map +
                '}';
    }
}
