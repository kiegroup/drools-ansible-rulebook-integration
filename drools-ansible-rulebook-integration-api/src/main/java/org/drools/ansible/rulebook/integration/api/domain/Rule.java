package org.drools.ansible.rulebook.integration.api.domain;

import java.util.List;

import org.drools.ansible.rulebook.integration.api.RuleConfigurationOptions;
import org.drools.ansible.rulebook.integration.api.domain.actions.Action;
import org.drools.ansible.rulebook.integration.api.domain.actions.MapAction;
import org.drools.ansible.rulebook.integration.api.domain.conditions.AstCondition;
import org.drools.ansible.rulebook.integration.api.domain.conditions.Condition;
import org.drools.ansible.rulebook.integration.api.domain.conditions.OnceWithinDefinition;
import org.drools.ansible.rulebook.integration.api.domain.conditions.TimeWindowDefinition;

public class Rule {
    private String name;
    private Condition condition;
    private Action action;
    private boolean enabled;

    private String onceWithin;
    private List<String> uniqueAttributes;

    private final RuleGenerationContext ruleGenerationContext = new RuleGenerationContext();

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

    public Rule withOptions(RuleConfigurationOptions options) {
        this.ruleGenerationContext.addOptions(options.getOptions());
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

    public void setOnce_within(String onceWithin) {
        this.onceWithin = onceWithin;
        if (uniqueAttributes != null) {
            ruleGenerationContext.setTimeConstraint(OnceWithinDefinition.parseOnceWithin(onceWithin, uniqueAttributes));
        }
    }

    public void setUnique_attributes(List<String> uniqueAttributes) {
        this.uniqueAttributes = uniqueAttributes;
        if (onceWithin != null) {
            ruleGenerationContext.setTimeConstraint(OnceWithinDefinition.parseOnceWithin(onceWithin, uniqueAttributes));
        }
    }

    public void setTime_window(String timeWindow) {
        ruleGenerationContext.setTimeConstraint(TimeWindowDefinition.parseTimeWindow((String) timeWindow));
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
