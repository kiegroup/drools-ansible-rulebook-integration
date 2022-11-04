package org.drools.ansible.rulebook.integration.api.domain.conditions;

import java.util.ArrayList;
import java.util.List;

import org.drools.ansible.rulebook.integration.api.RuleGenerationContext;
import org.drools.ansible.rulebook.integration.api.rulesmodel.ParsedCondition;
import org.drools.model.Index;
import org.drools.model.PrototypeDSL;
import org.drools.model.PrototypeExpression;
import org.drools.model.view.CombinedExprViewItem;
import org.drools.model.view.ViewItem;

import static org.drools.ansible.rulebook.integration.api.rulesmodel.PrototypeFactory.DEFAULT_PROTOTYPE_NAME;
import static org.drools.model.DSL.not;
import static org.drools.model.PrototypeDSL.variable;

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
            } else if (ruleContext.getOnceWithin() != null) {
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

    public static class AndCondition implements Condition {

        private Condition lhs;
        private Condition rhs;

        public AndCondition withLhs(Condition condition) {
            this.lhs = condition;
            return this;
        }

        public AndCondition withRhs(Condition condition) {
            this.rhs = condition;
            return this;
        }

        @Override
        public ViewItem toPattern(RuleGenerationContext ruleContext) {
            PrototypeDSL.PrototypePatternDef oldPattern = ruleContext.getCurrentPattern();
            ruleContext.setCurrentPattern((PrototypeDSL.PrototypePatternDef) lhs.toPattern(ruleContext));
            ViewItem result = rhs.toPattern(ruleContext);
            ruleContext.setCurrentPattern(oldPattern);
            return result;
        }
    }

    public static class OrCondition implements Condition {

        private Condition lhs;
        private Condition rhs;

        public OrCondition withLhs(Condition condition) {
            this.lhs = condition;
            return this;
        }

        public OrCondition withRhs(Condition condition) {
            this.rhs = condition;
            return this;
        }

        @Override
        public ViewItem toPattern(RuleGenerationContext ruleContext) {
            PrototypeDSL.PrototypePatternDef pattern = ruleContext.getOrCreatePattern(ruleContext.generateBinding(), DEFAULT_PROTOTYPE_NAME);
            pattern = pattern.or();
            ((SingleCondition) lhs).parsedCondition.addConditionToPattern(ruleContext, pattern);
            ((SingleCondition) rhs).parsedCondition.addConditionToPattern(ruleContext, pattern);
            return pattern.endOr();
        }
    }

    public static class SingleCondition<P extends MultipleConditions> implements Condition {

        private final P parent;

        private final ParsedCondition parsedCondition;

        private PrototypeDSL.PrototypePatternDef pattern;

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

        private PrototypeDSL.PrototypePatternDef getPattern(RuleGenerationContext ruleContext) {
            if (pattern == null) {
                pattern = ruleContext.getOrCreatePattern(ruleContext.generateBinding(), DEFAULT_PROTOTYPE_NAME);
            }
            return pattern;
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
            ViewItem pattern = parsedCondition.addConditionToPattern(ruleContext, getPattern(ruleContext));
            OnceWithinDefinition onceWithin = ruleContext.getOnceWithin();
            if (onceWithin != null) {
                return onceWithin.appendGuardPattern(ruleContext, pattern);
            }
            return pattern;
        }

        public SingleCondition<P> addSingleCondition(PrototypeExpression left, Index.ConstraintType operator, PrototypeExpression right) {
            return parent.addSingleCondition(left, operator, right);
        }
    }
}