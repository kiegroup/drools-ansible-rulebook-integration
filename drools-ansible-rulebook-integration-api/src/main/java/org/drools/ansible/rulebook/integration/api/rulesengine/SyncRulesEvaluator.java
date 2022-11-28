package org.drools.ansible.rulebook.integration.api.rulesengine;

import java.util.List;

import org.drools.ansible.rulebook.integration.api.io.RuleExecutorChannel;
import org.kie.api.runtime.rule.Match;

public class SyncRulesEvaluator extends AbstractRulesEvaluator {

    private RuleExecutorChannel channel;

    public SyncRulesEvaluator(RulesExecutorSession rulesExecutorSession) {
        super(rulesExecutorSession);
    }

    protected List<Match> writeResponse(List<Match> matches) {
        return matches;
    }
}
