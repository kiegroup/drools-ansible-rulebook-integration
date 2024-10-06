package org.drools.ansible.rulebook.integration.api.domain.constraints;

import org.drools.ansible.rulebook.integration.api.domain.RuleGenerationContext;
import org.drools.ansible.rulebook.integration.api.domain.conditions.ConditionExpression;
import org.drools.ansible.rulebook.integration.api.rulesmodel.ParsedCondition;
import org.drools.model.functions.Function1;
import org.drools.model.prototype.PrototypeExpression;
import org.drools.model.prototype.PrototypeVariable;
import org.kie.api.prototype.Prototype;
import org.kie.api.prototype.PrototypeFactInstance;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiPredicate;

import static org.drools.ansible.rulebook.integration.api.domain.conditions.ConditionExpression.map2Expr;
import static org.drools.model.prototype.PrototypeExpression.thisPrototype;

public enum NegatedExistsField implements RulebookOperator, ConditionFactory {

    INSTANCE;

    public static final String EXPRESSION_NAME = "IsNotDefinedExpression";

    private static final Object ADMITTED_UNDEFINED_VALUE = AdmittedUndefinedValue.INSTANCE;

    @Override
    public <T, V> BiPredicate<T, V> asPredicate() {
        return (t, v) -> v == ADMITTED_UNDEFINED_VALUE;
    }

    @Override
    public String toString() {
        return "NEGATED_EXISTS_FIELD";
    }

    @Override
    public ParsedCondition createParsedCondition(RuleGenerationContext ruleContext, String expressionName, Map<?, ?> expression) {
        PrototypeExpression rightExpression = map2Expr(ruleContext, expression).getPrototypeExpression();
        return new ParsedCondition(thisPrototype(), this, new PrototypeExpressionWithAdmittedUndefined(rightExpression));
    }

    private static class PrototypeExpressionWithAdmittedUndefined implements PrototypeExpression {
        private final PrototypeExpression delegate;

        private PrototypeExpressionWithAdmittedUndefined(PrototypeExpression delegate) {
            this.delegate = delegate;
        }

        @Override
        public Function1<PrototypeFactInstance, Object> asFunction(Prototype prototype) {
            return delegate.asFunction(prototype).andThen( result -> result == Prototype.UNDEFINED_VALUE ? ADMITTED_UNDEFINED_VALUE : result );
        }

        @Override
        public Collection<String> getImpactedFields() {
            return delegate.getImpactedFields();
        }

        @Override
        public Optional<String> getIndexingKey() {
            return delegate.getIndexingKey();
        }

        @Override
        public PrototypeExpression andThen(PrototypeExpression other) {
            return delegate.andThen(other);
        }

        @Override
        public boolean hasPrototypeVariable() {
            return delegate.hasPrototypeVariable();
        }

        @Override
        public Collection<PrototypeVariable> getPrototypeVariables() {
            return delegate.getPrototypeVariables();
        }

        @Override
        public PrototypeExpression composeWith(BinaryOperation.Operator op, PrototypeExpression right) {
            return delegate.composeWith(op, right);
        }

        @Override
        public PrototypeExpression add(PrototypeExpression right) {
            return delegate.add(right);
        }

        @Override
        public PrototypeExpression sub(PrototypeExpression right) {
            return delegate.sub(right);
        }

        @Override
        public PrototypeExpression mul(PrototypeExpression right) {
            return delegate.mul(right);
        }

        @Override
        public PrototypeExpression div(PrototypeExpression right) {
            return delegate.div(right);
        }
    }

    public static class AdmittedUndefinedValue {
        static final AdmittedUndefinedValue INSTANCE = new AdmittedUndefinedValue();
        static final UnsupportedOperationException HASHCODE_EXCEPTION = new UnsupportedOperationException();

        public AdmittedUndefinedValue() {
        }

        public int hashCode() {
            throw HASHCODE_EXCEPTION;
        }

        public boolean equals(Object obj) {
            return false;
        }

        public String toString() {
            return "$AdmittedUndefinedValue$";
        }
    }
}
