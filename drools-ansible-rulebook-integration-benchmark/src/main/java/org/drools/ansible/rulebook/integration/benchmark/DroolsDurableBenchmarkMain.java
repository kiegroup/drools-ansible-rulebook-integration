package org.drools.ansible.rulebook.integration.benchmark;

import org.drools.ansible.rulebook.integration.api.RulesExecutor;
import org.drools.ansible.rulebook.integration.api.RulesExecutorFactory;
import org.drools.ansible.rulebook.integration.durable.DurableNotation;

public class DroolsDurableBenchmarkMain {
    public static void main(String[] args) {
        String jsonRule = "{ \"rules\": {\"r_0\": {\"all\": [{\"m\": {\"$ex\": {\"event.i\": 1}}}]}}}";
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(DurableNotation.INSTANCE, jsonRule);
        rulesExecutor.processEvents("{ \"event\": { \"i\": \"Done\" } }");
    }
}
