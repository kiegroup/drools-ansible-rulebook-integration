package org.drools.ansible.rulebook.integration.api.rulesengine;

import java.text.SimpleDateFormat;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.drools.core.time.impl.PseudoClockScheduler;
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

    AutomaticPseudoClock(AbstractRulesEvaluator rulesEvaluator, long amount, TimeUnit unit) {
        this(rulesEvaluator, unit.toMillis(amount));
    }

    AutomaticPseudoClock(AbstractRulesEvaluator rulesEvaluator, long period) {
        this.period = period;
        this.rulesEvaluator = rulesEvaluator;
        this.nextTick = rulesEvaluator.getCurrentTime();
        if (PseudoClockScheduler.DEBUG) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            LOG.info("AutomaticPseudoClock<init> nextTick : {}", sdf.format(nextTick));
        }
        timer.scheduleAtFixedRate(this::advancePseudoClock, period, period, TimeUnit.MILLISECONDS);
    }

    public long getPeriod() {
        return period;
    }

    public void shutdown() {
        timer.shutdown();
    }

    private void advancePseudoClock() {
        nextTick += period;
        rulesEvaluator.scheduledAdvanceTimeToMills(nextTick);
    }
}
