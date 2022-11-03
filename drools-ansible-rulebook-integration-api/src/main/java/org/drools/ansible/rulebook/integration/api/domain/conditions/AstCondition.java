package org.drools.ansible.rulebook.integration.api.domain.conditions;

import java.util.ArrayList;
import java.util.List;

import org.drools.ansible.rulebook.integration.api.RuleGenerationContext;
import org.drools.ansible.rulebook.integration.api.rulesmodel.ParsedCondition;
import org.drools.model.Index;
import org.drools.model.PrototypeDSL;
import org.drools.model.PrototypeExpression;
import org.drools.model.PrototypeVariable;
import org.drools.model.view.CombinedExprViewItem;
import org.drools.model.view.ViewItem;

import static org.drools.ansible.rulebook.integration.api.rulesmodel.PrototypeFactory.DEFAULT_PROTOTYPE_NAME;
import static org.drools.ansible.rulebook.integration.api.rulesmodel.PrototypeFactory.SYNTHETIC_PROTOTYPE_NAME;
import static org.drools.model.DSL.not;
import static org.drools.model.PrototypeDSL.protoPattern;
import static org.drools.model.PrototypeDSL.variable;
import static org.drools.model.PrototypeExpression.prototypeField;

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

    public static abstract class MultipleConditions implements Condition {
        protected final RuleGenerationContext ruleContext;

        protected final List<Condition> conditions = new ArrayList<>();

        public MultipleConditions(RuleGenerationContext ruleContext) {
            this.ruleContext = ruleContext;
        }

        public MultipleConditions addCondition(Condition condition) {
            conditions.add(condition);
            return this;
        }

        public SingleCondition addSingleCondition(PrototypeExpression left, Index.ConstraintType operator, PrototypeExpression right) {
            SingleCondition singleCondition = new SingleCondition(left, operator, right);
            singleCondition.setRuleContext(ruleContext);
            return singleCondition;
        }
    }

    public static class AllCondition extends MultipleConditions {

        public AllCondition(RuleGenerationContext ruleContext) {
            super(ruleContext);
        }

        @Override
        public ViewItem toPattern(RuleGenerationContext ruleContext) {
            if (conditions.size() == 1) {
                return conditions.get(0).toPattern(ruleContext);
            } else if (ruleContext.getOnceWithin() != null) {
                throw new IllegalArgumentException("once_within is only allowed with a single event");
            }
            return new CombinedExprViewItem(org.drools.model.Condition.Type.AND, conditions.stream()
                    .map(subC -> subC.toPattern(ruleContext)).toArray(ViewItem[]::new));
        }
    }

    public static class AnyCondition extends MultipleConditions {

        public AnyCondition(RuleGenerationContext ruleContext) {
            super(ruleContext);
        }

        @Override
        public ViewItem toPattern(RuleGenerationContext ruleContext) {
            if (conditions.size() == 1) {
                return conditions.get(0).toPattern(ruleContext);
            }
            return new CombinedExprViewItem(org.drools.model.Condition.Type.OR, conditions.stream()
                    .map(subC -> toScopedPatten(ruleContext, subC)).toArray(ViewItem[]::new));
        }

        private static ViewItem toScopedPatten(RuleGenerationContext ruleContext, Condition subC) {
            ruleContext.pushContext();
            ViewItem result = subC.toPattern(ruleContext);
            ruleContext.pushContext();
            return result;
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

    public static class SingleCondition implements Condition {

        private final ParsedCondition parsedCondition;

        private PrototypeDSL.PrototypePatternDef pattern;

        private RuleGenerationContext ruleContext;

        public SingleCondition(PrototypeExpression left, Index.ConstraintType operator, PrototypeExpression right) {
            this(new ParsedCondition(left, operator, right));
        }

        public SingleCondition(ParsedCondition parsedCondition) {
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
            this.pattern = ruleContext.getOrCreatePattern(patternBinding, DEFAULT_PROTOTYPE_NAME);
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
    }
}