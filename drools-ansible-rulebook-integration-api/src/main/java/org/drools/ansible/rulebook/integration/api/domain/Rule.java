package org.drools.ansible.rulebook.integration.api.domain;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.drools.ansible.rulebook.integration.api.RuleConfigurationOption;
import org.drools.ansible.rulebook.integration.api.RuleConfigurationOptions;
import org.drools.ansible.rulebook.integration.api.domain.actions.Action;
import org.drools.ansible.rulebook.integration.api.domain.actions.MapAction;
import org.drools.ansible.rulebook.integration.api.domain.conditions.AstCondition;
import org.drools.ansible.rulebook.integration.api.domain.conditions.Condition;
import org.drools.ansible.rulebook.integration.api.domain.temporal.Throttle;
import org.drools.ansible.rulebook.integration.api.domain.temporal.TimeWindowDefinition;
import org.drools.ansible.rulebook.integration.api.domain.temporal.TimedOutDefinition;
import org.drools.ansible.rulebook.integration.api.rulesengine.RulesExecutionController;

public class Rule {
    private String name;
    private Condition condition;
    private Action action;
    private boolean enabled;

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

    public void setThrottle(Throttle throttle) {
        ruleGenerationContext.setTimeConstraint(throttle.asTimeConstraint(name));
    }

    public void setTimeout(String timeWindow) {
        ruleGenerationContext.setTimeConstraint(TimeWindowDefinition.parseTimeWindow(timeWindow));
    }

    public void setTimed_out(String timedOut) {
        ruleGenerationContext.setTimeConstraint(TimedOutDefinition.parseTimedOut(timedOut));
    }

    public boolean requiresAsyncExecution() {
        return ruleGenerationContext.requiresAsyncExecution();
    }

    @Override
    public String toString() {
        return "Rule{" +
                "name='" + name + '\'' +
                ", condition='" + condition + '\'' +
                ", action='" + action + '\'' +
                '}';
    }

    List<org.drools.model.Rule> toExecModelRules(RulesSet rulesSet, RulesExecutionController rulesExecutionController, AtomicInteger ruleCounter) {
        if (name == null) {
            name = "r_" + ruleCounter.getAndIncrement();
        }
        ruleGenerationContext.setRuleName(name);

        List<org.drools.model.Rule> rules = ruleGenerationContext.generateRules(rulesExecutionController, condition, action);
        if (ruleGenerationContext.hasTimeConstraint()) {
            rulesSet.withOptions(RuleConfigurationOption.EVENTS_PROCESSING);
        }
        return rules;
    }
}
