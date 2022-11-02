package org.drools.ansible.rulebook.integration.api.domain;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.drools.ansible.rulebook.integration.api.RuleNotation;
import org.drools.ansible.rulebook.integration.api.RulesExecutor;
import org.drools.ansible.rulebook.integration.api.rulesmodel.PrototypeFactory;
import org.drools.model.impl.ModelImpl;
import org.drools.modelcompiler.KieBaseBuilder;
import org.kie.api.KieBase;
import org.kie.api.conf.KieBaseMutabilityOption;

import static org.drools.model.DSL.execute;
import static org.drools.model.PatternDSL.rule;

@JsonIgnoreProperties(ignoreUnknown = true)
public class RulesSet {
    private String name;
    private List<String> hosts;
    private List<RuleContainer> rules;

    private RuleNotation.RuleConfigurationOption[] options;

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

    public KieBase toKieBase(Supplier<RulesExecutor> rulesExecutorSupplier) {
        AtomicInteger ruleCounter = new AtomicInteger(0);

        ModelImpl model = new ModelImpl();
        rules.stream().map(RuleContainer::getRule)
                .map(r -> r.withRuleGenerationContext(prototypeFactory, options))
                .map(rule -> toExecModelRule(rule, rulesExecutorSupplier, ruleCounter))
                .forEach(model::addRule);
        KieBase kieBase = KieBaseBuilder.createKieBaseFromModel( model, KieBaseMutabilityOption.DISABLED );
        return kieBase;
    }

    private static org.drools.model.Rule toExecModelRule(Rule rule, Supplier<RulesExecutor> rulesExecutorSupplier, AtomicInteger ruleCounter) {
        return rule( rule.getName() != null ? rule.getName() : "r_" + ruleCounter.getAndIncrement() )
                .build( rule.getCondition().toPattern( rule.getRuleGenerationContext() ),
                        execute(drools -> rule.getAction().execute(rulesExecutorSupplier.get(), drools)) );
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
        rule.withRuleGenerationContext(prototypeFactory, options);
        RuleContainer ruleContainer = new RuleContainer();
        ruleContainer.setRule(rule);
        if (rules == null) {
            rules = new ArrayList<>();
        }
        rules.add(ruleContainer);
        return rule;
    }

    public RuleNotation.RuleConfigurationOption[] getOptions() {
        return options;
    }

    public PrototypeFactory getPrototypeFactory() {
        return prototypeFactory;
    }

    public RulesSet withOptions(RuleNotation.RuleConfigurationOption[] options) {
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
