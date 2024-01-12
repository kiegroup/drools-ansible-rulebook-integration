package org.drools.ansible.rulebook.integration.api.rulesengine;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.drools.base.facttemplates.Fact;

import java.time.Instant;
import java.util.Collection;
import java.util.stream.Stream;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class SessionStats {
    private final String start;
    private final String end;

    private final String lastClockTime;
    private final int clockAdvanceCount;

    private final int numberOfRules;
    private final int numberOfDisabledRules;
    private final int rulesTriggered;

    private final int eventsProcessed;
    private final int eventsMatched;
    private final int eventsSuppressed;

    private final int permanentStorageCount;
    private final int permanentStorageSize;

    private final int asyncResponses;
    private final int bytesSentOnAsync;

    private final long sessionId;

    private final String ruleSetName;

    private final String lastRuleFired;
    private final String lastRuleFiredAt;
    private final String lastEventReceivedAt;

    public SessionStats(SessionStatsCollector stats, RulesExecutorSession session, boolean disposing) {
        this.start = stats.getStart().toString();
        this.end = disposing ? Instant.now().toString() : null;
        this.lastClockTime = Instant.ofEpochMilli(session.getPseudoClock().getCurrentTime()).toString();
        this.clockAdvanceCount = stats.getClockAdvanceCount();
        this.numberOfRules = session.rulesCount();
        this.numberOfDisabledRules = session.disabledRulesCount();
        this.rulesTriggered = stats.getRulesTriggered();
        this.eventsProcessed = stats.getTotalEvents();
        this.eventsMatched = stats.getMatchedEvents();
        this.eventsSuppressed = this.eventsProcessed - this.eventsMatched;

        Collection<Fact> facts = (Collection<Fact>) session.getObjects(Fact.class::isInstance);
        this.permanentStorageCount = facts.size();
        Stream<Fact> factStream = permanentStorageCount > Runtime.getRuntime().availableProcessors() * 2 ? facts.parallelStream() : facts.stream();
        this.permanentStorageSize = factStream.mapToInt(f -> f.asMap().toString().getBytes().length).sum();

        this.asyncResponses = stats.getAsyncResponses();
        this.bytesSentOnAsync = stats.getBytesSentOnAsync();
        this.sessionId = session.getId();
        this.ruleSetName = session.getRuleSetName();
        this.lastRuleFired = stats.getLastRuleFired();
        this.lastRuleFiredAt = stats.getLastRuleFiredTime() < 0 ? null : Instant.ofEpochMilli(stats.getLastRuleFiredTime()).toString();
        this.lastEventReceivedAt = stats.getLastEventReceivedTime() < 0 ? null : Instant.ofEpochMilli(stats.getLastEventReceivedTime()).toString();
    }

    public SessionStats(String start, String end, String lastClockTime, int clockAdvanceCount, int numberOfRules, int numberOfDisabledRules, int rulesTriggered, int eventsProcessed,
                        int eventsMatched, int eventsSuppressed, int permanentStorageCount, int permanentStorageSize, int asyncResponses, int bytesSentOnAsync,
                        long sessionId, String ruleSetName, String lastRuleFired, String lastRuleFiredAt, String lastEventReceivedAt) {
        this.start = start;
        this.end = end;
        this.lastClockTime = lastClockTime;
        this.clockAdvanceCount = clockAdvanceCount;
        this.numberOfRules = numberOfRules;
        this.numberOfDisabledRules = numberOfDisabledRules;
        this.rulesTriggered = rulesTriggered;
        this.eventsProcessed = eventsProcessed;
        this.eventsMatched = eventsMatched;
        this.eventsSuppressed = eventsSuppressed;
        this.permanentStorageCount = permanentStorageCount;
        this.permanentStorageSize = permanentStorageSize;
        this.asyncResponses = asyncResponses;
        this.bytesSentOnAsync = bytesSentOnAsync;
        this.sessionId = sessionId;
        this.ruleSetName = ruleSetName;
        this.lastRuleFired = lastRuleFired;
        this.lastRuleFiredAt = lastRuleFiredAt;
        this.lastEventReceivedAt = lastEventReceivedAt;
    }

    @Override
    public String toString() {
        return "SessionStats{" +
                "start='" + start + '\'' +
                (end != null ? ", end='" + end + '\'' : "") +
                ", lastClockTime='" + lastClockTime + '\'' +
                ", clockAdvanceCount=" + clockAdvanceCount +
                ", numberOfRules=" + numberOfRules +
                ", numberOfDisabledRules=" + numberOfDisabledRules +
                ", rulesTriggered=" + rulesTriggered +
                ", eventsProcessed=" + eventsProcessed +
                ", eventsMatched=" + eventsMatched +
                ", eventsSuppressed=" + eventsSuppressed +
                ", permanentStorageCount=" + permanentStorageCount +
                ", permanentStorageSize=" + permanentStorageSize +
                ", asyncResponses=" + asyncResponses +
                ", bytesSentOnAsync=" + bytesSentOnAsync +
                ", sessionId=" + sessionId +
                ", ruleSetName='" + ruleSetName + '\'' +
                ", lastRuleFired='" + lastRuleFired + '\'' +
                ", lastRuleFiredAt='" + lastRuleFiredAt + '\'' +
                ", lastEventReceivedAt='" + lastEventReceivedAt + '\'' +
                ", usedMemory='" + getUsedMemory() + '\'' +
                ", maxAvailableMemory='" + getMaxAvailableMemory() + '\'' +
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

    public int getPermanentStorageCount() {
        return permanentStorageCount;
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

    public String getLastClockTime() {
        return lastClockTime;
    }

    public String getLastRuleFired() {
        return lastRuleFired;
    }

    public String getLastRuleFiredAt() {
        return lastRuleFiredAt;
    }

    public String getLastEventReceivedAt() {
        return lastEventReceivedAt;
    }

    public int getClockAdvanceCount() {
        return clockAdvanceCount;
    }

    public long getUsedMemory() {
        return MemoryMonitorUtil.getUsedMemory();
    }

    public long getMaxAvailableMemory() {
        return MemoryMonitorUtil.getMaxAvailableMemory();
    }

    public static SessionStats aggregate(SessionStats stats1, SessionStats stats2) {
        String lastRuleFired = null;
        String lastRuleFiredAt = null;

        if (isInstant1Last(stats1.getLastRuleFiredAt(), stats2.getLastRuleFiredAt())) {
            lastRuleFired = stats1.getLastRuleFired();
            lastRuleFiredAt = stats1.getLastRuleFiredAt();
        } else {
            lastRuleFired = stats2.getLastRuleFired();
            lastRuleFiredAt = stats2.getLastRuleFiredAt();
        }

        return new SessionStats(
                Instant.parse(stats1.getStart()).compareTo(Instant.parse(stats2.getStart())) < 0 ? stats1.getStart() : stats2.getStart(),
                stats1.getEnd() == null || stats2.getEnd() == null ? // if one of the sessions is still alive the aggregate is also alive
                        null :
                        ( Instant.parse(stats1.getEnd()).compareTo(Instant.parse(stats2.getEnd())) > 0 ? stats1.getEnd() : stats2.getEnd() ),
                Instant.parse(stats1.getLastClockTime()).compareTo(Instant.parse(stats2.getLastClockTime())) > 0 ? stats1.getLastClockTime() : stats2.getLastClockTime(),
                stats1.clockAdvanceCount + stats2.clockAdvanceCount,
                stats1.numberOfRules + stats2.numberOfRules,
                stats1.numberOfDisabledRules + stats2.numberOfDisabledRules,
                stats1.rulesTriggered + stats2.rulesTriggered,
                stats1.eventsProcessed + stats2.eventsProcessed,
                stats1.eventsMatched + stats2.eventsMatched,
                stats1.eventsSuppressed + stats2.eventsSuppressed,
                stats1.permanentStorageCount + stats2.permanentStorageCount,
                stats1.permanentStorageSize + stats2.permanentStorageSize,
                stats1.asyncResponses + stats2.asyncResponses,
                stats1.bytesSentOnAsync + stats2.bytesSentOnAsync,
                -1,
                stats1.getRuleSetName().equals(stats2.getRuleSetName()) ? stats1.getRuleSetName() : "",
                lastRuleFired,
                lastRuleFiredAt,
                isInstant1Last(stats1.getLastEventReceivedAt(), stats2.getLastEventReceivedAt()) ? stats1.getLastEventReceivedAt() : stats2.getLastEventReceivedAt()
        );
    }

    private static boolean isInstant1Last(String instant1, String instant2) {
        return instant1 != null && (instant2 == null || Instant.parse(instant1).compareTo(Instant.parse(instant2)) > 0);
    }

}
