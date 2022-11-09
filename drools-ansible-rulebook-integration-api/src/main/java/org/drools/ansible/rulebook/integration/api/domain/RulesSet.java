package org.drools.ansible.rulebook.integration.api.domain;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.drools.ansible.rulebook.integration.api.RuleConfigurationOption;
import org.drools.ansible.rulebook.integration.api.RuleConfigurationOptions;
import org.drools.ansible.rulebook.integration.api.RulesExecutionController;
import org.drools.model.Drools;
import org.drools.model.Model;
import org.drools.model.RuleItemBuilder;
import org.drools.model.impl.ModelImpl;

import static org.drools.model.DSL.execute;
import static org.drools.model.DSL.on;
import static org.drools.model.PatternDSL.rule;

@JsonIgnoreProperties(ignoreUnknown = true)
public class RulesSet {
    private String name;
    private List<String> hosts;
    private List<RuleContainer> rules;

    private RuleConfigurationOptions options;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<String> getHosts() {
        return hosts;
    }

    public void setHosts(List<String> hosts) {
        this.hosts = hosts;
    }

    public Model toExecModel(RulesExecutionController rulesExecutionController) {
        AtomicInteger ruleCounter = new AtomicInteger(0);

        ModelImpl model = new ModelImpl();
        rules.stream().map(RuleContainer::getRule)
                .map(r -> r.createRuleGenerationContext(options))
                .flatMap(rule -> toExecModelRules(rule, rulesExecutionController, ruleCounter).stream())
                .forEach(model::addRule);
        return model;
    }

    private List<org.drools.model.Rule> toExecModelRules(Rule rule, RulesExecutionController rulesExecutionController, AtomicInteger ruleCounter) {
        RuleGenerationContext ruleContext = rule.getRuleGenerationContext();
        RuleItemBuilder pattern = rule.getCondition().toPattern( ruleContext );
        RuleItemBuilder consequence;
        if ( ruleContext.getConsequenceVariable() != null ) {
            consequence = on(ruleContext.getConsequenceVariable()).execute((drools, fact) -> {
                ruleContext.executeSyntheticConsequence(drools, fact);
                defaultConsequence(rule, rulesExecutionController, drools);
            });
        } else {
            consequence = execute(drools -> defaultConsequence(rule, rulesExecutionController, drools));
        }

        if (ruleContext.hasTimeConstraint()) {
            withOptions(RuleConfigurationOption.EVENTS_PROCESSING);
        }

        String ruleName = rule.getName() != null ? rule.getName() : "r_" + ruleCounter.getAndIncrement();
        org.drools.model.Rule generatedRule = rule( ruleName ).build( pattern, consequence );
        org.drools.model.Rule syntheticRule = ruleContext.getSyntheticRule();
        return syntheticRule == null ? Collections.singletonList(generatedRule) : Arrays.asList( generatedRule, syntheticRule );
    }

    private static void defaultConsequence(Rule rule, RulesExecutionController rulesExecutionController, Drools drools) {
        if (rulesExecutionController.executeActions()) {
            rule.getAction().execute(drools);
        }
    }

    public void setRules(List<RuleContainer> rules) {
        this.rules = rules;
    }

    public Rule addRule() {
        return addRule(null);
    }

    public Rule addRule(String name) {
        Rule rule = new Rule();
        rule.setName(name);
        rule.createRuleGenerationContext(options);
        RuleContainer ruleContainer = new RuleContainer();
        ruleContainer.setRule(rule);
        if (rules == null) {
            rules = new ArrayList<>();
        }
        rules.add(ruleContainer);
        return rule;
    }

    public boolean hasOption(RuleConfigurationOption option) {
        return options != null && options.hasOption(option);
    }

    public RulesSet withOptions(RuleConfigurationOption... options) {
        if (this.options == null) {
            this.options = new RuleConfigurationOptions(options);
        } else {
            for (RuleConfigurationOption option : options) {
                this.options.addOption(option);
            }
        }
        return this;
    }

    @Override
    public String toString() {
        return "RulesSet{" +
                "name='" + name + '\'' +
                ", hosts='" + hosts + '\'' +
                ", rules=" + rules +
                '}';
    }
}
