package org.drools.ansible.rulebook.integration.api.domain.conditions;

import org.drools.model.view.ViewItem;
import org.drools.ansible.rulebook.integration.api.RuleGenerationContext;

public interface Condition {
    ViewItem toPattern(RuleGenerationContext ruleContext);
}
