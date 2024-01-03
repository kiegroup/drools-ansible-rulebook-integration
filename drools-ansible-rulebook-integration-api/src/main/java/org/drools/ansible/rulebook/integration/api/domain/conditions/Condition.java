package org.drools.ansible.rulebook.integration.api.domain.conditions;

import org.drools.ansible.rulebook.integration.api.domain.RuleGenerationContext;
import org.drools.model.view.ViewItem;

public interface Condition {
    ViewItem toPattern(RuleGenerationContext ruleContext);
}
