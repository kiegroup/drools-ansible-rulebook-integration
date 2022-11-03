package org.drools.ansible.rulebook.integration.api.domain;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.drools.ansible.rulebook.integration.api.RuleConfigurationOption;
import org.drools.ansible.rulebook.integration.api.RuleConfigurationOptions;
import org.drools.ansible.rulebook.integration.api.RuleGenerationContext;
import org.drools.ansible.rulebook.integration.api.RulesExecutor;
import org.drools.ansible.rulebook.integration.api.rulesmodel.PrototypeFactory;
import org.drools.model.Drools;
import org.drools.model.Model;
import org.drools.model.RuleItemBuilder;
import org.drools.model.impl.ModelImpl;
import org.drools.modelcompiler.KieBaseBuilder;
import org.kie.api.KieBase;
import org.kie.api.conf.EventProcessingOption;
import org.kie.api.conf.KieBaseMutabilityOption;

import static org.drools.model.DSL.execute;
import static org.drools.model.DSL.on;
import static org.drools.model.PatternDSL.rule;

@JsonIgnoreProperties(ignoreUnknown = true)
public class RulesSet {
    private String name;
    private List<String> hosts;
    private List<RuleContainer> rules;

    private RuleConfigurationOptions options;

    private final PrototypeFactory prototypeFactory = new PrototypeFactory();

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

    public Model toExecModel(Supplier<RulesExecutor> rulesExecutorSupplier) {
        AtomicInteger ruleCounter = new AtomicInteger(0);

        ModelImpl model = new ModelImpl();
        rules.stream().map(RuleContainer::getRule)
                .map(r -> r.createRuleGenerationContext(prototypeFactory, options))
                .flatMap(rule -> toExecModelRules(rule, rulesExecutorSupplier, ruleCounter).stream())
                .forEach(model::addRule);
        return model;
    }

    private static List<org.drools.model.Rule> toExecModelRules(Rule rule, Supplier<RulesExecutor> rulesExecutorSupplier, AtomicInteger ruleCounter) {
        RuleGenerationContext ruleContext = rule.getRuleGenerationContext();
        RuleItemBuilder pattern = rule.getCondition().toPattern( ruleContext );
        RuleItemBuilder consequence;
        if ( ruleContext.getConsequenceVariable() != null ) {
            consequence = on(ruleContext.getConsequenceVariable()).execute((drools, fact) -> {
                ruleContext.executeSyntheticConsequence(drools, fact);
                defaultConsequence(rule, rulesExecutorSupplier, drools);
            });
        } else {
            consequence = execute(drools -> defaultConsequence(rule, rulesExecutorSupplier, drools));
        }

        String ruleName = rule.getName() != null ? rule.getName() : "r_" + ruleCounter.getAndIncrement();
        org.drools.model.Rule generatedRule = rule( ruleName ).build( pattern, consequence );
        org.drools.model.Rule syntheticRule = ruleContext.getSyntheticRule();
        return syntheticRule == null ? Arrays.asList( generatedRule ) : Arrays.asList( generatedRule, syntheticRule );
    }

    private static void defaultConsequence(Rule rule, Supplier<RulesExecutor> rulesExecutorSupplier, Drools drools) {
        RulesExecutor rulesExecutor = rulesExecutorSupplier.get();
        if (rulesExecutor.executeActions()) {
            rule.getAction().execute(rulesExecutor, drools);
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
        rule.createRuleGenerationContext(prototypeFactory, options);
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

    public PrototypeFactory getPrototypeFactory() {
        return prototypeFactory;
    }

    public RulesSet withOptions(RuleConfigurationOptions options) {
        this.options = options;
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
