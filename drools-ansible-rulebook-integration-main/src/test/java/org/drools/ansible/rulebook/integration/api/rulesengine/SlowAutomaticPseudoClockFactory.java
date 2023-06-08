package org.drools.ansible.rulebook.integration.api.rulesengine;

import java.util.concurrent.TimeUnit;

public class SlowAutomaticPseudoClockFactory implements AutomaticPseudoClockFactory {

    @Override
    public AutomaticPseudoClock createAutomaticPseudoClock(AbstractRulesEvaluator rulesEvaluator, long amount, TimeUnit unit) {
        return new SlowAutomaticPseudoClock(rulesEvaluator, amount, unit);
    }
}
