package org.drools.ansible.rulebook.integration.api.domain;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.drools.ansible.rulebook.integration.api.RuleConfigurationOptions;
import org.drools.ansible.rulebook.integration.api.domain.actions.Action;
import org.drools.ansible.rulebook.integration.api.domain.actions.MapAction;
import org.drools.ansible.rulebook.integration.api.domain.conditions.AstCondition;
import org.drools.ansible.rulebook.integration.api.domain.conditions.Condition;
import org.drools.ansible.rulebook.integration.api.domain.temporal.Throttle;
import org.drools.ansible.rulebook.integration.api.domain.temporal.TimeWindowDefinition;
import org.drools.ansible.rulebook.integration.api.rulesengine.RulesExecutionController;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Rule {

    private boolean enabled = true;

    private final RuleGenerationContext ruleGenerationContext = new RuleGenerationContext();

    private String ruleSetName;

    private String name;

    private Action action;

    private List<MapAction> actions;

    private RuleConfigurationOptions options;

    private Condition condition;

    public String getRuleSetName() {
        return ruleSetName;
    }

    public void setRuleSetName(String ruleSetName) {
        this.ruleSetName = ruleSetName;
    }

    public String getName() {
    	return name;
    }

    public void setName(String name) {
    	this.name = name;
    }

    public Rule withOptions(RuleConfigurationOptions options) {
    	this.options = options;
        return this;
    }
    
    public RuleConfigurationOptions getOptions() {
    	return options;
    }

    public Condition getCondition() {
    	return condition;
    }
    
    public void setCondition(Condition condition) {
        this.condition = condition;
    }

    public AstCondition withCondition() {
        AstCondition condition = new AstCondition(ruleGenerationContext);
        this.condition = condition;
        return condition;
    }

    // If "actions" are used in JSON, then getAction returns the first action in the list
    public Action getAction() {
    	return action;
    }

    public void setAction(MapAction action) {
        this.action = action;

        // If "action" is used in JSON, actions has the single action
        this.actions = new ArrayList<>();
        this.actions.add(action);
    }

    public void setActions(List<MapAction> actions) {
        this.actions = actions;

        // If "actions" are used in JSON, action has the first action in the list
        if (actions != null && !actions.isEmpty()) {
            this.action = actions.get(0);
        }
    }

    public List<MapAction> getActions() {
        return actions;
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
        ruleGenerationContext.setTimeConstraint(throttle.asTimeConstraint());
    }

    public void setTimeout(String timeWindow) {
        ruleGenerationContext.setTimeConstraint(TimeWindowDefinition.parseTimeWindow(timeWindow));
    }

    public boolean hasTemporalConstraint() {
        return ruleGenerationContext.hasTemporalConstraint(this);
    }

    public boolean requiresAsyncExecution() {
        return ruleGenerationContext.requiresAsyncExecution(this);
    }
    
    public List<org.drools.model.Rule> toExecModelRules(RulesSet rulesSet, RulesExecutionController rulesExecutionController, AtomicInteger ruleCounter) {
    	return ruleGenerationContext.toExecModelRules(rulesSet, this, rulesExecutionController, ruleCounter);
    	
    }

    @Override
    public String toString() {
        return "Rule{" +
                "name='" + ruleGenerationContext.getRuleName() + '\'' +
                ", condition='" + ruleGenerationContext.getCondition() + '\'' +
                ", action='" + ruleGenerationContext.getAction() + '\'' +
                '}';
    }
}
