package org.drools.ansible.rulebook.integration.api.domain.conditions;

import java.util.ArrayList;
import java.util.List;

import org.drools.ansible.rulebook.integration.api.domain.RuleGenerationContext;
import org.drools.ansible.rulebook.integration.api.domain.temporal.OnceWithinDefinition;
import org.drools.ansible.rulebook.integration.api.rulesmodel.ParsedCondition;
import org.drools.model.Index;
import org.drools.model.PrototypeDSL;
import org.drools.model.PrototypeExpression;
import org.drools.model.view.CombinedExprViewItem;
import org.drools.model.view.ViewItem;

import static org.drools.ansible.rulebook.integration.api.rulesmodel.PrototypeFactory.DEFAULT_PROTOTYPE_NAME;

public class AstCondition implements Condition {

    private final RuleGenerationContext ruleContext;

    private Condition rootCondition;

    public AstCondition(RuleGenerationContext ruleContext) {
        this.ruleContext = ruleContext;
    }

    public AllCondition all() {
        AllCondition allCondition = new AllCondition(ruleContext);
        this.rootCondition = allCondition;
        return allCondition;
    }

    public AnyCondition any() {
        AnyCondition anyCondition = new AnyCondition(ruleContext);
        this.rootCondition = anyCondition;
        return anyCondition;
    }

    @Override
    public ViewItem toPattern(RuleGenerationContext ruleContext) {
        return rootCondition.toPattern(ruleContext);
    }

    public static abstract class MultipleConditions<T extends MultipleConditions> implements Condition {
        protected final RuleGenerationContext ruleContext;

        protected final List<Condition> conditions = new ArrayList<>();

        public MultipleConditions(RuleGenerationContext ruleContext) {
            this.ruleContext = ruleContext;
        }

        protected abstract org.drools.model.Condition.Type getConditionType();

        @Override
        public ViewItem toPattern(RuleGenerationContext ruleContext) {
            if (conditions.size() == 1) {
                return conditions.get(0).toPattern(ruleContext);
            } else if (ruleContext.getTimeConstraint().map(tc -> tc instanceof OnceWithinDefinition).orElse(false)) {
                throw new IllegalArgumentException("once_within is only allowed with a single event");
            }
            return new CombinedExprViewItem(getConditionType(), conditions.stream()
                    .map(subC -> subC.toPattern(ruleContext)).toArray(ViewItem[]::new));
        }

        public MultipleConditions addCondition(Condition condition) {
            conditions.add(condition);
            return this;
        }

        public SingleCondition<T> addSingleCondition(PrototypeExpression left, Index.ConstraintType operator, PrototypeExpression right) {
            SingleCondition<T> singleCondition = new SingleCondition(this, left, operator, right);
            singleCondition.setRuleContext(ruleContext);
            conditions.add(singleCondition);
            return singleCondition;
        }

        protected void beforeBinding() { }
        protected void afterBinding() { }
    }

    public static class AllCondition extends MultipleConditions<AllCondition> {

        public AllCondition(RuleGenerationContext ruleContext) {
            super(ruleContext);
        }

        @Override
        protected org.drools.model.Condition.Type getConditionType() {
            return org.drools.model.Condition.Type.AND;
        }
    }

    public static class AnyCondition extends MultipleConditions<AnyCondition> {

        public AnyCondition(RuleGenerationContext ruleContext) {
            super(ruleContext);
        }

        @Override
        protected org.drools.model.Condition.Type getConditionType() {
            return org.drools.model.Condition.Type.OR;
        }

        @Override
        protected void beforeBinding() {
            ruleContext.pushContext();
        }

        @Override
        protected void afterBinding() {
            ruleContext.popContext();
        }
    }

    public abstract static class PatternCondition implements Condition {
        protected PrototypeDSL.PrototypePatternDef pattern;

        abstract ViewItem addConditionToPattern(RuleGenerationContext ruleContext, PrototypeDSL.PrototypePatternDef pattern);

        abstract PatternCondition negate(RuleGenerationContext ruleContext);

        protected ViewItem processTimeConstraint(RuleGenerationContext ruleContext, ViewItem pattern) {
            return ruleContext.getTimeConstraint().map(tc -> tc.processTimeConstraint(ruleContext.getRuleName(), pattern)).orElse(pattern);
        }

        protected PrototypeDSL.PrototypePatternDef getPattern(RuleGenerationContext ruleContext) {
            return pattern != null ? pattern : getPattern(ruleContext, ruleContext.generateBinding());
        }

        protected PrototypeDSL.PrototypePatternDef getPattern(RuleGenerationContext ruleContext, String binding) {
            if (pattern == null) {
                pattern = ruleContext.getOrCreatePattern(binding, DEFAULT_PROTOTYPE_NAME);
            }
            return pattern;
        }
    }

    public abstract static class CombinedPatternCondition extends PatternCondition {

        protected final String binding;

        protected PatternCondition lhs;
        protected PatternCondition rhs;

        protected CombinedPatternCondition(String binding) {
            this.binding = binding;
        }

        public CombinedPatternCondition withLhs(PatternCondition condition) {
            this.lhs = condition;
            return this;
        }

        public CombinedPatternCondition withRhs(PatternCondition condition) {
            this.rhs = condition;
            return this;
        }
    }

    public static class AndCondition extends CombinedPatternCondition {

        public AndCondition(String binding) {
            super(binding);
        }

        @Override
        public ViewItem toPattern(RuleGenerationContext ruleContext) {
            PrototypeDSL.PrototypePatternDef pattern = getPattern(ruleContext, binding);
            addConditionToPattern(ruleContext, pattern);
            return processTimeConstraint(ruleContext, pattern);
        }

        @Override
        public ViewItem addConditionToPattern(RuleGenerationContext ruleContext, PrototypeDSL.PrototypePatternDef pattern) {
            lhs.addConditionToPattern(ruleContext, pattern);
            return rhs.addConditionToPattern(ruleContext, pattern);
        }

        @Override
        public PatternCondition negate(RuleGenerationContext ruleContext) {
            return new OrCondition(binding)
                    .withLhs(lhs.negate(ruleContext))
                    .withRhs(rhs.negate(ruleContext));
        }
    }

    public static class OrCondition extends CombinedPatternCondition {

        public OrCondition(String binding) {
            super(binding);
        }

        @Override
        public ViewItem toPattern(RuleGenerationContext ruleContext) {
            PrototypeDSL.PrototypePatternDef pattern = getPattern(ruleContext, binding);
            addConditionToPattern(ruleContext, pattern);
            return processTimeConstraint(ruleContext, pattern);
        }

        @Override
        public ViewItem addConditionToPattern(RuleGenerationContext ruleContext, PrototypeDSL.PrototypePatternDef pattern) {
            pattern = pattern.or();
            PrototypeDSL.PrototypePatternDef p1 = pattern.and();
            lhs.addConditionToPattern(ruleContext, p1);
            p1.endAnd();
            PrototypeDSL.PrototypePatternDef p2 = pattern.and();
            rhs.addConditionToPattern(ruleContext, p2);
            p2.endAnd();
            pattern = (PrototypeDSL.PrototypePatternDef) pattern.endOr();
            return pattern;
        }

        @Override
        public PatternCondition negate(RuleGenerationContext ruleContext) {
            return new AndCondition(binding)
                    .withLhs(lhs.negate(ruleContext))
                    .withRhs(rhs.negate(ruleContext));
        }
    }

    public static class SingleCondition<P extends MultipleConditions> extends PatternCondition {

        private final P parent;

        private final ParsedCondition parsedCondition;

        private RuleGenerationContext ruleContext;

        public SingleCondition(P parent, PrototypeExpression left, Index.ConstraintType operator, PrototypeExpression right) {
            this(parent, new ParsedCondition(left, operator, right));
        }

        public SingleCondition(P parent, ParsedCondition parsedCondition) {
            this.parent = parent;
            this.parsedCondition = parsedCondition;
        }

        void setRuleContext(RuleGenerationContext ruleContext) {
            this.ruleContext = ruleContext;
        }

        public SingleCondition withPatternBinding(String patternBinding) {
            return withPatternBinding(this.ruleContext, patternBinding);
        }

        SingleCondition withPatternBinding(RuleGenerationContext ruleContext, String patternBinding) {
            if (parent != null) {
                parent.beforeBinding();
            }
            this.pattern = ruleContext.getOrCreatePattern(patternBinding, DEFAULT_PROTOTYPE_NAME);
            if (parent != null) {
                parent.afterBinding();
            }
            return this;
        }

        @Override
        public ViewItem toPattern(RuleGenerationContext ruleContext) {
            ViewItem pattern = addConditionToPattern(ruleContext, getPattern(ruleContext));
            return processTimeConstraint(ruleContext, pattern);
        }

        @Override
        public ViewItem addConditionToPattern(RuleGenerationContext ruleContext, PrototypeDSL.PrototypePatternDef pattern) {
            return parsedCondition.addConditionToPattern(ruleContext, pattern);
        }

        @Override
        public PatternCondition negate(RuleGenerationContext ruleContext) {
            parsedCondition.negate();
            return this;
        }

        public SingleCondition<P> addSingleCondition(PrototypeExpression left, Index.ConstraintType operator, PrototypeExpression right) {
            return parent.addSingleCondition(left, operator, right);
        }
    }
}