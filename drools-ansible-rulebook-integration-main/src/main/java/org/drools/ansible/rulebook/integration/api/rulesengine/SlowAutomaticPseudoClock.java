package org.drools.ansible.rulebook.integration.api.rulesengine;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SlowAutomaticPseudoClock extends AutomaticPseudoClock {

    private static final Logger LOG = LoggerFactory.getLogger(SlowAutomaticPseudoClock.class.getName());

    public static final String DELAY_PROPERTY = "SlowAutomaticPseudoClock.delay";
    private final long delay = Long.getLong(DELAY_PROPERTY, 0);
    public static final String AMOUNT_PROPERTY = "SlowAutomaticPseudoClock.amount";
    private final long amount = Long.getLong(AMOUNT_PROPERTY, 0);
    private boolean slownessInduced = false;
    private final long startTime;

    public SlowAutomaticPseudoClock(AbstractRulesEvaluator rulesEvaluator, long amount, TimeUnit unit) {
        super(rulesEvaluator, amount, unit);
        startTime = nextTick;
    }

    @Override
    protected void advancePseudoClock() {
        super.advancePseudoClock();

        if (!slownessInduced && nextTick >= startTime + delay) {
            try {
                LOG.info("Inducing slowness of {} ms", amount);
                Thread.sleep(amount);
                slownessInduced = true;
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
