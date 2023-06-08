package org.drools.ansible.rulebook.integration.api.rulesengine;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SlowAutomaticPseudoClock extends AutomaticPseudoClock {

    private static final Logger LOG = LoggerFactory.getLogger(SlowAutomaticPseudoClock.class.getName());

    private static boolean enabled = false; // when false, this should work as the same as AutomaticPseudoClock
    private static long delay = 0;
    private static long amount = 0;
    private boolean slownessInduced = false;
    private final long startTime;

    public SlowAutomaticPseudoClock(AbstractRulesEvaluator rulesEvaluator, long amount, TimeUnit unit) {
        super(rulesEvaluator, amount, unit);
        startTime = nextTick;
    }

    @Override
    protected void advancePseudoClock() {
        super.advancePseudoClock();

        if (enabled && !slownessInduced && nextTick >= startTime + delay) {
            try {
                LOG.info("Inducing slowness of {} ms", amount);
                Thread.sleep(amount);
                slownessInduced = true;
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static void enable(long delayValue, long amountValue) {
        enabled = true;
        delay = delayValue;
        amount = amountValue;
    }

    public static void resetAndDisable() {
        enabled = false;
        delay = 0;
        amount = 0;
    }
}
