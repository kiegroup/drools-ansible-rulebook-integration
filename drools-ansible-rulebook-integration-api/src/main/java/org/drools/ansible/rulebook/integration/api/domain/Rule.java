package org.drools.ansible.rulebook.integration.api.domain;

import org.drools.ansible.rulebook.integration.api.RuleConfigurationOptions;
import org.drools.ansible.rulebook.integration.api.domain.actions.Action;
import org.drools.ansible.rulebook.integration.api.domain.actions.MapAction;
import org.drools.ansible.rulebook.integration.api.domain.conditions.AstCondition;
import org.drools.ansible.rulebook.integration.api.domain.conditions.Condition;

public class Rule {
    private String name;
    private Condition condition;
    private Action action;
    private boolean enabled;

    private RuleGenerationContext ruleGenerationContext;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Condition getCondition() {
        return condition;
    }

    public void setCondition(Condition condition) {
        this.condition = condition;
    }

    public Rule createRuleGenerationContext(RuleConfigurationOptions options) {
        this.ruleGenerationContext = new RuleGenerationContext(options);
        return this;
    }

    public RuleGenerationContext getRuleGenerationContext() {
        return ruleGenerationContext;
    }

    public boolean hasTimeConstraint() {
        return ruleGenerationContext.hasTimeConstraint();
    }

    public AstCondition withCondition() {
        condition = new AstCondition(ruleGenerationContext);
        return (AstCondition) condition;
    }

    public Action getAction() {
        return action;
    }

    public void setAction(MapAction action) {
        this.action = action;
    }

    public void setGenericAction(Action action) {
        this.action = action;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public boolean isEnabled() {
        return enabled;
    }

    @Override
    public String toString() {
        return "Rule{" +
                "name='" + name + '\'' +
                ", condition='" + condition + '\'' +
                ", action='" + action + '\'' +
                '}';
    }
}
