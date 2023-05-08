package org.drools.ansible.rulebook.integration.api.rulesengine;

import org.drools.core.facttemplates.Event;

import java.time.Instant;

import static java.util.function.Predicate.not;

public class SessionStats {
    private final String start;
    private final String end;

    private final int numberOfRules;
    private final int numberOfDisabledRules;
    private final int rulesTriggered;

    private final int eventsProcessed;
    private final int eventsMatched;
    private final int eventsSuppressed;

    private final int permanentStorageSize;

    private final int asyncResponses;
    private final int bytesSentOnAsync;

    private final long sessionId;

    private final String ruleSetName;

    public SessionStats(SessionStatsCollector stats, RulesExecutorSession session) {
        this.start = stats.getStart().toString();
        this.end = Instant.now().toString();
        this.numberOfRules = session.rulesCount();
        this.numberOfDisabledRules = session.disabledRulesCount();
        this.rulesTriggered = stats.getRulesTriggered();
        this.eventsProcessed = stats.getTotalEvents();
        this.eventsMatched = stats.getMatchedEvents();
        this.eventsSuppressed = this.eventsProcessed - this.eventsMatched;
        this.permanentStorageSize = (int) session.getObjects().stream().filter(not(Event.class::isInstance)).count();
        this.asyncResponses = stats.getAsyncResponses();
        this.bytesSentOnAsync = stats.getBytesSentOnAsync();
        this.sessionId = session.getId();
        this.ruleSetName = session.getRuleSetName();

    }

    public SessionStats(String start, String end, int numberOfRules, int numberOfDisabledRules, int rulesTriggered, int eventsProcessed,
                        int eventsMatched, int eventsSuppressed, int permanentStorageSize, int asyncResponses, int bytesSentOnAsync,
                        long sessionId, String ruleSetName) {
        this.start = start;
        this.end = end;
        this.numberOfRules = numberOfRules;
        this.numberOfDisabledRules = numberOfDisabledRules;
        this.rulesTriggered = rulesTriggered;
        this.eventsProcessed = eventsProcessed;
        this.eventsMatched = eventsMatched;
        this.eventsSuppressed = eventsSuppressed;
        this.permanentStorageSize = permanentStorageSize;
        this.asyncResponses = asyncResponses;
        this.bytesSentOnAsync = bytesSentOnAsync;
        this.sessionId = sessionId;
        this.ruleSetName = ruleSetName;
    }

    @Override
    public String toString() {
        return "SessionStats{" +
                "start=" + start +
                ", end=" + end +
                ", numberOfRules=" + numberOfRules +
                ", rulesTriggered=" + rulesTriggered +
                ", eventsProcessed=" + eventsProcessed +
                ", eventsMatched=" + eventsMatched +
                ", eventsSuppressed=" + eventsSuppressed +
                ", permanentStorageSize=" + permanentStorageSize +
                ", asyncResponses=" + asyncResponses +
                ", bytesSentOnAsync=" + bytesSentOnAsync +
                '}';
    }

    public String getStart() {
        return start;
    }

    public String getEnd() {
        return end;
    }

    public int getNumberOfRules() {
        return numberOfRules;
    }

    public int getNumberOfDisabledRules() {
        return numberOfDisabledRules;
    }

    public int getRulesTriggered() {
        return rulesTriggered;
    }

    public int getEventsProcessed() {
        return eventsProcessed;
    }

    public int getEventsMatched() {
        return eventsMatched;
    }

    public int getEventsSuppressed() {
        return eventsSuppressed;
    }

    public int getPermanentStorageSize() {
        return permanentStorageSize;
    }

    public int getAsyncResponses() {
        return asyncResponses;
    }

    public int getBytesSentOnAsync() {
        return bytesSentOnAsync;
    }

    public long getSessionId() {
        return sessionId;
    }

    public String getRuleSetName() {
        return ruleSetName;
    }

    public static SessionStats aggregate(SessionStats stats1, SessionStats stats2) {
        return new SessionStats(
                stats1.getStart().compareTo(stats2.getStart()) < 0 ? stats1.getStart() : stats2.getStart(),
                stats1.getEnd().compareTo(stats2.getEnd()) > 0 ? stats1.getStart() : stats2.getStart(),
                stats1.numberOfRules + stats2.numberOfRules,
                stats1.numberOfDisabledRules + stats2.numberOfDisabledRules,
                stats1.rulesTriggered + stats2.rulesTriggered,
                stats1.eventsProcessed + stats2.eventsProcessed,
                stats1.eventsMatched + stats2.eventsMatched,
                stats1.eventsSuppressed + stats2.eventsSuppressed,
                stats1.permanentStorageSize + stats2.permanentStorageSize,
                stats1.asyncResponses + stats2.asyncResponses,
                stats1.bytesSentOnAsync + stats2.bytesSentOnAsync,
                -1,
                stats1.getRuleSetName().equals(stats2.getRuleSetName()) ? stats1.getRuleSetName() : ""
        );
    }
}
