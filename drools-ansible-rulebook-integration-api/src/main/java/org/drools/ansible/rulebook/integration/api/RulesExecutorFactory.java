package org.drools.ansible.rulebook.integration.api;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.drools.ansible.rulebook.integration.api.rulesmodel.PrototypeFactory;
import org.drools.model.impl.ModelImpl;
import org.drools.modelcompiler.KieBaseBuilder;
import org.drools.ansible.rulebook.integration.api.domain.Rule;
import org.drools.ansible.rulebook.integration.api.domain.RulesSet;
import org.kie.api.KieBase;
import org.kie.api.conf.KieBaseMutabilityOption;

import static org.drools.model.DSL.execute;
import static org.drools.model.PatternDSL.rule;

public class RulesExecutorFactory {

    private static final AtomicLong ID_GENERATOR = new AtomicLong(1);

    public static RulesExecutor createFromYaml(String yaml) {
        return createFromYaml(RuleNotation.CoreNotation.INSTANCE, yaml);
    }

    public static RulesExecutor createFromYaml(RuleNotation notation, String yaml) {
        return create(RuleFormat.YAML, notation, yaml);
    }

    public static RulesExecutor createFromJson(String json) {
        return createFromJson(RuleNotation.CoreNotation.INSTANCE, json);
    }

    public static RulesExecutor createFromJson(RuleNotation notation, String json) {
        return create(RuleFormat.JSON, notation, json);
    }

    private static RulesExecutor create(RuleFormat format, RuleNotation notation, String text) {
        return createRulesExecutor(notation.toRulesSet(format, text));
    }

    public static RulesExecutor createRulesExecutor(RulesSet rulesSet) {
        RulesExecutor rulesExecutor = new RulesExecutor(createRulesExecutorSession(rulesSet), ID_GENERATOR.getAndIncrement());
        return RulesExecutorContainer.INSTANCE.register(rulesExecutor);
    }

    public static RulesExecutorSession createRulesExecutorSession(RulesSet rulesSet) {
        RulesExecutorSession.RulesExecutorHolder rulesExecutorHolder = new RulesExecutorSession.RulesExecutorHolder();
        KieBase kieBase = rulesSet.toKieBase(rulesExecutorHolder);
        return new RulesExecutorSession(rulesSet.getPrototypeFactory(), kieBase.newKieSession(), rulesExecutorHolder);
    }
}
