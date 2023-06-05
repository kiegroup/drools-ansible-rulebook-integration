package org.drools.ansible.rulebook.integration.api.rulesengine;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AutomaticPseudoClock {

    private static final Logger LOG = LoggerFactory.getLogger(AutomaticPseudoClock.class);

    private final ScheduledThreadPoolExecutor timer = new ScheduledThreadPoolExecutor(1, r -> {
        Thread t = new Thread(r);
        t.setDaemon(true);
        return t;
    });

    private final AbstractRulesEvaluator rulesEvaluator;

    private final long period;

    private volatile long nextTick;

    private volatile long lastCurrentTime;

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

    private void advancePseudoClock() {
        long currentTime = rulesEvaluator.getCurrentTime();
        if (currentTime == lastCurrentTime) {
            LOG.info("advanceTime is stuck at {}. Skipping", currentTime); // consider log level debug?
            return;
        }
        lastCurrentTime = currentTime;

        nextTick = System.currentTimeMillis();
        rulesEvaluator.scheduledAdvanceTimeToMills(nextTick);
    }
}
