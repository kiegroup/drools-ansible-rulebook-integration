package org.drools.ansible.rulebook.integration.durable.domain;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.drools.ansible.rulebook.integration.api.domain.Rule;
import org.drools.ansible.rulebook.integration.api.domain.RuleContainer;
import org.drools.ansible.rulebook.integration.api.domain.RulesSet;

public class DurableRules extends HashMap<String, Map<String, DurableRule>> {

    public RulesSet toRulesSet() {
        RulesSet rulesSet = new RulesSet();
        rulesSet.setName(keySet().iterator().next());

        List<RuleContainer> rules = new ArrayList<>();
        for (Map.Entry<String, DurableRule> ruleEntry : values().iterator().next().entrySet()) {
            Rule rule = ruleEntry.getValue().toRule();
            rule.setName(ruleEntry.getKey());
            rules.add(new RuleContainer(rule));
        }
        rulesSet.setRules(rules);

        return rulesSet;
    }
}
