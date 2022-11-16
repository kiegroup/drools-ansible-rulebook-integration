package org.drools.ansible.rulebook.integration.api;

import java.util.concurrent.atomic.AtomicLong;

import org.drools.core.ClockType;
import org.drools.model.Model;
import org.drools.modelcompiler.KieBaseBuilder;
import org.drools.ansible.rulebook.integration.api.domain.RulesSet;
import org.kie.api.KieBase;
import org.kie.api.KieServices;
import org.kie.api.conf.EventProcessingOption;
import org.kie.api.conf.KieBaseMutabilityOption;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.KieSessionConfiguration;
import org.kie.api.runtime.conf.ClockTypeOption;

import static org.drools.ansible.rulebook.integration.api.RuleConfigurationOption.EVENTS_PROCESSING;
import static org.drools.ansible.rulebook.integration.api.RuleConfigurationOption.USE_PSEUDO_CLOCK;
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

    private static RulesExecutorSession createRulesExecutorSession(RulesSet rulesSet) {
        RulesExecutionController rulesExecutionController = new RulesExecutionController();
        KieSession kieSession = createKieSession(rulesSet, rulesExecutionController);
        return new RulesExecutorSession(kieSession, rulesExecutionController);
    }

    private static KieSession createKieSession(RulesSet rulesSet, RulesExecutionController rulesExecutionController) {
        Model model = rulesSet.toExecModel(rulesExecutionController);
        KieBase kieBase = KieBaseBuilder.createKieBaseFromModel( model,
                KieBaseMutabilityOption.DISABLED,
                rulesSet.hasOption(EVENTS_PROCESSING) ? EventProcessingOption.STREAM : EventProcessingOption.CLOUD );

        if (rulesSet.hasOption(USE_PSEUDO_CLOCK)) {
            KieSessionConfiguration conf = KieServices.get().newKieSessionConfiguration();
            conf.setOption(ClockTypeOption.get(ClockType.PSEUDO_CLOCK.getId()));
            return kieBase.newKieSession(conf, null);
        }
        return kieBase.newKieSession();
    }
}
