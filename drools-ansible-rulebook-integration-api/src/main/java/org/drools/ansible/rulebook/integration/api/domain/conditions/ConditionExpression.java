package org.drools.ansible.rulebook.integration.api.domain.conditions;

import org.drools.ansible.rulebook.integration.api.domain.RuleGenerationContext;
import org.drools.ansible.rulebook.integration.protoextractor.prototype.ExtractorPrototypeExpressionUtils;
import org.drools.model.prototype.PrototypeDSL;
import org.drools.model.prototype.PrototypeExpression;
import org.drools.model.prototype.PrototypeVariable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.drools.ansible.rulebook.integration.api.domain.conditions.ConditionParseUtil.isEventOrFact;
import static org.drools.ansible.rulebook.integration.api.domain.conditions.ConditionParseUtil.isKnownType;
import static org.drools.ansible.rulebook.integration.api.domain.conditions.ConditionParseUtil.toJsonValue;
import static org.drools.model.prototype.PrototypeExpression.fixedValue;

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
            PrototypeExpression composed = ExtractorPrototypeExpressionUtils.prototypeFieldExtractor(betaVariable, getFieldName())
                    .composeWith(decodeBinaryOperator, ExtractorPrototypeExpressionUtils.prototypeFieldExtractor(rhs.getBetaVariable(), rhs.getFieldName()));
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
        return prototypeExpression.getIndexingKey().orElseThrow(IllegalStateException::new);
    }

    public PrototypeExpression getPrototypeExpression() {
        return prototypeExpression;
    }

    public boolean isField() {
        return field;
    }

    public boolean isFixedValue() {
        return prototypeExpression instanceof PrototypeExpression.FixedValue;
    }

    public Object getFixedValue() {
        return ((PrototypeExpression.FixedValue) prototypeExpression).getValue();
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
            String fieldName = isEventOrFact(key) ? (String) value : key + "." + value;
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
        return new ConditionExpression(ExtractorPrototypeExpressionUtils.prototypeFieldExtractor(fieldName), true, prototypeName, betaVariable);
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
