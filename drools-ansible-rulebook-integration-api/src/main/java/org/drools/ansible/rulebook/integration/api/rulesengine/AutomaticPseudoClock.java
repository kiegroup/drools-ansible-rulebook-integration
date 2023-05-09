package org.drools.ansible.rulebook.integration.api.rulesengine;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class AutomaticPseudoClock {

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
