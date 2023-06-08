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
        nextTick += period;
        if (Math.abs(System.currentTimeMillis() - nextTick) > period * 2) {
            LOG.warn("Pseudo clock is diverged, the difference is {} ms", (System.currentTimeMillis() - nextTick));
        }
        rulesEvaluator.scheduledAdvanceTimeToMills(nextTick);
    }
}
