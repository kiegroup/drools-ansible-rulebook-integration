package org.drools.ansible.rulebook.integration.api.domain.conditions;

import org.drools.model.PrototypeDSL;
import org.drools.model.view.ViewItem;
import org.drools.ansible.rulebook.integration.api.domain.RuleGenerationContext;

public interface Condition {
    ViewItem toPattern(RuleGenerationContext ruleContext);
}
