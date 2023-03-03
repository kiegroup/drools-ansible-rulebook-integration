package org.drools.ansible.rulebook.integration.benchmark;

import org.drools.ansible.rulebook.integration.api.RulesExecutor;
import org.drools.ansible.rulebook.integration.api.RulesExecutorFactory;

import static org.drools.ansible.rulebook.integration.benchmark.DroolsBenchmark.JSON_RULE;

public class DroolsBenchmarkMain {
    public static void main(String[] args) {
        RulesExecutor rulesExecutor = RulesExecutorFactory.createFromJson(JSON_RULE);
        rulesExecutor.processEvents("{ \"i\": \"Done\" }");
    }
}
