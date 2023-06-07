package org.drools.ansible.rulebook.integration.main;

import org.drools.ansible.rulebook.integration.api.rulesengine.AbstractRulesEvaluator;
import org.drools.ansible.rulebook.integration.api.rulesengine.SlowAutomaticPseudoClock;
import org.junit.Test;

public class SlownessTest {

    @Test
    public void testOnceAfter() {
        try {
            System.setProperty(AbstractRulesEvaluator.AUTOMATIC_PSEUDO_CLOCK_CUSTOM_CLASS_NAME_PROPERTY, SlowAutomaticPseudoClock.class.getName());
            System.setProperty(SlowAutomaticPseudoClock.DELAY_PROPERTY, "11000");
            System.setProperty(SlowAutomaticPseudoClock.AMOUNT_PROPERTY, "5000");
            Main.execute("56_once_after.json");

            // No assertions, just check a warning. TODO : modify Main so that we can assert matches
            // WARN org.drools.ansible.rulebook.integration.api.rulesengine.SessionStatsCollector - r1 is fired with a delay of 4013 ms

        } finally {
            System.clearProperty(AbstractRulesEvaluator.AUTOMATIC_PSEUDO_CLOCK_CUSTOM_CLASS_NAME_PROPERTY);
            System.clearProperty(SlowAutomaticPseudoClock.DELAY_PROPERTY);
            System.clearProperty(SlowAutomaticPseudoClock.AMOUNT_PROPERTY);
        }
    }
}
