package org.drools.ansible.rulebook.integration.api.domain;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.drools.ansible.rulebook.integration.api.RuleConfigurationOption;
import org.drools.ansible.rulebook.integration.api.RuleConfigurationOptions;
import org.drools.ansible.rulebook.integration.api.RulesExecutionController;
import org.drools.model.Model;
import org.drools.model.impl.ModelImpl;

@JsonIgnoreProperties(ignoreUnknown = true)
public class RulesSet {
    private String name;
    private List<String> hosts;
    private List<RuleContainer> rules;

    private final RuleConfigurationOptions options = new RuleConfigurationOptions();

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
                .map(r -> r.withOptions(options))
                .flatMap(rule -> rule.toExecModelRules(this, rulesExecutionController, ruleCounter).stream())
                .forEach(model::addRule);
        return model;
    }

    public void setRules(List<RuleContainer> rules) {
        this.rules = rules;
    }

    public Rule addRule() {
        return addRule(null);
    }

    public Rule addRule(String name) {
        Rule rule = new Rule().withOptions(options);
        rule.setName(name);
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
        this.options.addOptions(options);
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
