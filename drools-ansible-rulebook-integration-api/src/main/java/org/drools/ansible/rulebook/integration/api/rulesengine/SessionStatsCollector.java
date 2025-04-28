package org.drools.ansible.rulebook.integration.api.rulesengine;

import org.kie.api.runtime.rule.FactHandle;
import org.kie.api.runtime.rule.Match;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

public class SessionStatsCollector {

    private static final Logger LOG = LoggerFactory.getLogger(SessionStatsCollector.class.getName());

    public static final String DELAY_WARNING_THRESHOLD_PROPERTY = "drools.delay.warning.threshold";
    private static final long DELAY_WARNING_THRESHOLD;

    private final long id;

    private final Instant start = Instant.now();

    private int rulesTriggered;

    private int totalEvents;
    private int matchedEvents;

    private int asyncResponses;
    private int bytesSentOnAsync;

    private String lastRuleFired = "";
    private long lastRuleFiredTime = -1;
    private long lastEventReceivedTime = -1;

    private int clockAdvanceCount;

    private long baseLevelMemory;

    static {
        String envValue = System.getenv("DROOLS_LOG_DELAY");
        if (envValue != null && !envValue.isEmpty()) {
            // Environment variable takes precedence over system property
            System.setProperty(DELAY_WARNING_THRESHOLD_PROPERTY, envValue);
        }
        DELAY_WARNING_THRESHOLD = Integer.getInteger(DELAY_WARNING_THRESHOLD_PROPERTY, 5) * 1000L; // ms
    }

    public SessionStatsCollector(long id) {
        this.id = id;
    }

    public SessionStats generateStats(RulesExecutorSession session, boolean disposing) {
        return new SessionStats(this, session, disposing);
    }

    public Instant getStart() {
        return start;
    }

    public int getRulesTriggered() {
        return rulesTriggered;
    }

    public int getTotalEvents() {
        return totalEvents;
    }

    public int getMatchedEvents() {
        return matchedEvents;
    }

    public int getAsyncResponses() {
        return asyncResponses;
    }

    public int getBytesSentOnAsync() {
        return bytesSentOnAsync;
    }

    public String getLastRuleFired() {
        return lastRuleFired;
    }

    public long getLastRuleFiredTime() {
        return lastRuleFiredTime;
    }

    public long getLastEventReceivedTime() {
        return lastEventReceivedTime;
    }

    public int getClockAdvanceCount() {
        return clockAdvanceCount;
    }

    public long getBaseLevelMemory() {
        return baseLevelMemory;
    }

    public void registerMatch(RulesExecutorSession session, Match match) {
        rulesTriggered++;
        lastRuleFired = match.getRule().getName();
        lastRuleFiredTime = session.getPseudoClock().getCurrentTime();
        long delay = System.currentTimeMillis() - lastRuleFiredTime;
        if (delay > DELAY_WARNING_THRESHOLD) {
            LOG.warn("{} is fired with a delay of {} ms", lastRuleFired, delay);
        }
    }

    public void registerMatchedEvents(Collection<FactHandle> events) {
        matchedEvents += events.size();
    }

    public void registerProcessedEvent(RulesExecutorSession session, FactHandle fh) {
        lastEventReceivedTime = session.getPseudoClock().getCurrentTime();
        totalEvents++;
    }

    public void registerAsyncResponse(byte[] bytes) {
        asyncResponses++;
        bytesSentOnAsync += bytes.length;
    }

    public void registerClockAdvance(long amount, TimeUnit unit) {
        clockAdvanceCount++;
    }

    public void registerBaseLevelMemory() {
        System.gc(); // NOSONAR
        this.baseLevelMemory = MemoryMonitorUtil.getUsedMemory();
    }
}
