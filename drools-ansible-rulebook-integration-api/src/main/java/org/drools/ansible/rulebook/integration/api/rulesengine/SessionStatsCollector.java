package org.drools.ansible.rulebook.integration.api.rulesengine;

import java.time.Instant;
import java.util.List;

import org.kie.api.runtime.rule.FactHandle;
import org.kie.api.runtime.rule.Match;

public class SessionStatsCollector {

    private final long id;

    private final Instant start = Instant.now();

    private int rulesTriggered;

    private int totalEvents;
    private int matchedEvents;

    private int asyncResponses;
    private int bytesSentOnAsync;

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

    public void registerMatch(Match match) {
        rulesTriggered++;
    }

    public void registerMatchedEvents(List<FactHandle> events) {
        matchedEvents += events.size();
    }

    public void registerProcessedEvent(FactHandle fh) {
        totalEvents++;
    }

    public void registerAsyncResponse(byte[] bytes) {
        asyncResponses++;
        bytesSentOnAsync += bytes.length;
    }
}
