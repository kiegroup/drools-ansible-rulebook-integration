package org.drools.ansible.rulebook.integration.api.rulesengine;

import java.util.List;

import org.drools.ansible.rulebook.integration.api.RulesExecutorContainer;
import org.drools.ansible.rulebook.integration.api.io.AstRuleMatch;
import org.drools.ansible.rulebook.integration.api.io.Response;
import org.drools.ansible.rulebook.integration.api.io.RuleExecutorChannel;
import org.kie.api.runtime.rule.Match;

public class AsyncRulesEvaluator extends AbstractRulesEvaluator {

    private RuleExecutorChannel channel;

    public AsyncRulesEvaluator(RulesExecutorSession rulesExecutorSession) {
        super(rulesExecutorSession);
    }

    public long getSessionId() {
        return rulesExecutorSession.getId();
    }


    @Override
    public void setRulesExecutorContainer(RulesExecutorContainer rulesExecutorContainer) {
        super.setRulesExecutorContainer(rulesExecutorContainer);
        this.channel = rulesExecutorContainer.getChannel();
    }

    protected List<Match> writeResponse(List<Match> matches) {
        // TODO: in case of async execution also make async the rule evaluation
        if (!matches.isEmpty()) { // skip empty result
            channel.write(new Response(getSessionId(), AstRuleMatch.asList(matches)));
        }
        return matches;
    }
}
