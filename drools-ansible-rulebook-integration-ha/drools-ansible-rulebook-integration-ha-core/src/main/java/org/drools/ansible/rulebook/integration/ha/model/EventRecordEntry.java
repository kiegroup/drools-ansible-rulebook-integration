package org.drools.ansible.rulebook.integration.ha.model;

import java.util.Objects;

public class EventRecordEntry {

    private final String recordIdentifier;

    private final long recordSequence;

    private final EventRecord record;

    private final String eventRecordSHA;

    public EventRecordEntry(String recordIdentifier, long recordSequence, EventRecord record) {
        this(recordIdentifier, recordSequence, record, null);
    }

    public EventRecordEntry(String recordIdentifier, long recordSequence, EventRecord record, String eventRecordSHA) {
        this.recordIdentifier = Objects.requireNonNull(recordIdentifier, "recordIdentifier must not be null");
        this.recordSequence = recordSequence;
        this.record = record;
        this.eventRecordSHA = eventRecordSHA;
    }

    public String getRecordIdentifier() {
        return recordIdentifier;
    }

    public long getRecordSequence() {
        return recordSequence;
    }

    public EventRecord getRecord() {
        return record;
    }

    public String getEventRecordSHA() {
        return eventRecordSHA;
    }
}
