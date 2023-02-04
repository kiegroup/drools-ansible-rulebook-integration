package org.drools.ansible.rulebook.integration.api.domain.conditions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.drools.ansible.rulebook.integration.api.domain.RuleGenerationContext;
import org.drools.model.PrototypeDSL;
import org.drools.model.PrototypeExpression;
import org.drools.model.PrototypeVariable;

import static org.drools.ansible.rulebook.integration.api.domain.conditions.ConditionParseUtil.isKnownType;
import static org.drools.ansible.rulebook.integration.api.domain.conditions.ConditionParseUtil.toJsonValue;
import static org.drools.model.PrototypeDSL.fieldName2PrototypeExpression;
import static org.drools.model.PrototypeExpression.fixedValue;
import static org.drools.model.PrototypeExpression.prototypeField;

public class ConditionExpression {

    private final PrototypeExpression prototypeExpression;
    private final boolean field;
    private final String prototypeName;
    private final PrototypeVariable betaVariable;

    public ConditionExpression(PrototypeExpression prototypeExpression) {
        this(prototypeExpression, false, null, null);
    }

    public ConditionExpression(PrototypeExpression prototypeExpression, boolean field, String prototypeName, PrototypeVariable betaVariable) {
        this.prototypeExpression = prototypeExpression;
        this.field = field;
        this.prototypeName = prototypeName;
        this.betaVariable = betaVariable;
    }

    public ConditionExpression composeWith(PrototypeExpression.BinaryOperation.Operator decodeBinaryOperator, ConditionExpression rhs) {
        if (isBeta() && rhs.isBeta() && !prototypeName.equals(rhs.getPrototypeName())) {
            PrototypeExpression composed = prototypeField(betaVariable, getFieldName())
                    .composeWith(decodeBinaryOperator, prototypeField(rhs.getBetaVariable(), rhs.getFieldName()));
            return new ConditionExpression(composed);
        }

        PrototypeExpression composed = prototypeExpression.composeWith(decodeBinaryOperator, rhs.prototypeExpression);
        if (field || rhs.field) {
            return new ConditionExpression(composed, true, prototypeName, betaVariable);
        }
        return new ConditionExpression(composed);
    }

    public boolean isBeta() {
        return betaVariable != null;
    }

    public String getFieldName() {
        return ((PrototypeExpression.PrototypeFieldValue) prototypeExpression).getFieldName();
    }

    public PrototypeExpression getPrototypeExpression() {
        return prototypeExpression;
    }

    public boolean isField() {
        return field;
    }

    public String getPrototypeName() {
        return prototypeName;
    }

    public PrototypeVariable getBetaVariable() {
        return betaVariable;
    }

    public static ConditionExpression map2Expr(RuleGenerationContext ruleContext, Object expr) {
        if (expr instanceof String) {
            return createFieldExpression(ruleContext, (String)expr);
        }

        if (expr instanceof Collection) {
            List list = new ArrayList();
            for (Object item : (Collection)expr) {
                Map.Entry itemEntry = ((Map<?,?>) item).entrySet().iterator().next();
                if (isKnownType( (String) itemEntry.getKey() )) {
                    list.add( toJsonValue( itemEntry ) );
                } else {
                    throw new UnsupportedOperationException("Unknown list item: " + itemEntry);
                }
            }
            return new ConditionExpression(fixedValue(list));
        }

        Map<?,?> exprMap = (Map) expr;
        assert(exprMap.size() == 1);
        Map.Entry entry = exprMap.entrySet().iterator().next();
        return mapEntry2Expr(ruleContext, expr, entry);
    }

    public static ConditionExpression mapEntry2Expr(RuleGenerationContext ruleContext, Map.Entry entry) {
        return mapEntry2Expr(ruleContext, null, entry);
    }

    public static ConditionExpression mapEntry2Expr(RuleGenerationContext ruleContext, Object expr, Map.Entry entry) {
        String key = (String) entry.getKey();
        Object value = entry.getValue();

        if (isKnownType(key)) {
            return new ConditionExpression(fixedValue(toJsonValue(key, value)));
        }

        if (value instanceof String) {
            String fieldName = ignoreKey(key) ? (String) value : key + "." + value;
            return createFieldExpression(ruleContext, fieldName);
        }

        if (value instanceof Map) {
            Map<?,?> expression = (Map<?,?>) value;
            return map2Expr(ruleContext, expression.get("lhs")).composeWith(decodeBinaryOperator(key), map2Expr(ruleContext, expression.get("rhs")));
        }

        throw new UnsupportedOperationException("Invalid expression: " + (expr != null ? expr : entry));
    }

    private static ConditionExpression createFieldExpression(RuleGenerationContext ruleContext, String fieldName) {
        int dotPos = fieldName.indexOf('.');
        String prototypeName = fieldName;
        PrototypeVariable betaVariable = null;
        if (dotPos > 0) {
            prototypeName = fieldName.substring(0, dotPos);
            PrototypeDSL.PrototypePatternDef boundPattern = ruleContext.getBoundPattern(prototypeName);
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
        return "ConditionExpression{" +
                "prototypeExpression=" + prototypeExpression +
                ", field=" + field +
                ", prototypeName='" + prototypeName + '\'' +
                '}';
    }
}
