package org.drools.ansible.rulebook.integration.api.rulesengine;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AutomaticPseudoClock {

    private static final Logger LOG = LoggerFactory.getLogger(AutomaticPseudoClock.class.getName());

    private final ScheduledThreadPoolExecutor timer = new ScheduledThreadPoolExecutor(1, r -> {
        Thread t = new Thread(r);
        t.setDaemon(true);
        return t;
    });

    private final AbstractRulesEvaluator rulesEvaluator;

    private final long period;

    protected volatile long nextTick;

    AutomaticPseudoClock(AbstractRulesEvaluator rulesEvaluator, long amount, TimeUnit unit) {
        this(rulesEvaluator, unit.toMillis(amount));
    }

    AutomaticPseudoClock(AbstractRulesEvaluator rulesEvaluator, long period) {
        this.period = period;
        this.rulesEvaluator = rulesEvaluator;
        this.nextTick = rulesEvaluator.getCurrentTime();
        timer.scheduleAtFixedRate(this::advancePseudoClock, period, period, TimeUnit.MILLISECONDS);
    }

    public long getPeriod() {
        return period;
    }

    public void shutdown() {
        timer.shutdown();
    }

    protected void advancePseudoClock() {

        long diff = System.currentTimeMillis() - nextTick;
        if (diff > period * 2) {
            LOG.warn("Pseudo clock is diverged, the difference is {} ms. Going to sync with the real clock.", diff);
            // DROOLS-7569 : Do not rely on "catch up" mechanism of ScheduledThreadPoolExecutor.scheduleAtFixedRate().
            // This is explicit "catch up" logic.
            // Also do not leap to the current time at once, because it may miss some rule firings.
            for (; nextTick < System.currentTimeMillis(); nextTick += period) {
                rulesEvaluator.scheduledAdvanceTimeToMills(nextTick);
            }
            return;
        } else if (diff < 0) {
            // This could happen when ScheduledThreadPoolExecutor piles up tasks because of the above "catch up" logic. Just ignore them.
            return;
        }

        // Normal case
        nextTick += period;
        rulesEvaluator.scheduledAdvanceTimeToMills(nextTick);
    }
}
