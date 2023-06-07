package org.drools.ansible.rulebook.integration.api.rulesengine;

import java.time.Instant;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import org.kie.api.runtime.rule.FactHandle;
import org.kie.api.runtime.rule.Match;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SessionStatsCollector {

    private static final Logger LOG = LoggerFactory.getLogger(SessionStatsCollector.class.getName());

    private final long id;

    private final Instant start = Instant.now();

    private int rulesTriggered;

    private int totalEvents;
    private int matchedEvents;

    private int asyncResponses;
    private int bytesSentOnAsync;

    private String lastRuleFired = "";
    private long lastRuleFiredTime;

    private int clockAdvanceCount;

    public SessionStatsCollector(long id) {
        this.id = id;
    }

    public SessionStats generateStats(RulesExecutorSession session) {
        return new SessionStats(this, session);
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

    public int getClockAdvanceCount() {
        return clockAdvanceCount;
    }

    public void registerMatch(RulesExecutorSession session, Match match) {
        rulesTriggered++;
        lastRuleFired = match.getRule().getName();
        lastRuleFiredTime = session.getPseudoClock().getCurrentTime();
        long delay = System.currentTimeMillis() - lastRuleFiredTime;
        if (delay > 0) {
            LOG.warn("{} is fired with a delay of {} ms", lastRuleFired, delay);
        }
    }

    public void registerMatchedEvents(Collection<FactHandle> events) {
        matchedEvents += events.size();
    }

    public void registerProcessedEvent(FactHandle fh) {
        totalEvents++;
    }

    public void registerAsyncResponse(byte[] bytes) {
        asyncResponses++;
        bytesSentOnAsync += bytes.length;
    }

    public void registerClockAdvance(long amount, TimeUnit unit) {
        clockAdvanceCount++;
    }
}
