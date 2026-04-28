package org.drools.ansible.rulebook.integration.ha.model;

import java.util.Objects;

public class EventRecordChange {

    public enum Type {
        UPSERT,
        DELETE
    }

    private final Type type;

    private final String recordIdentifier;

    private final EventRecordEntry entry;

    private EventRecordChange(Type type, String recordIdentifier, EventRecordEntry entry) {
        this.type = Objects.requireNonNull(type, "type must not be null");
        this.recordIdentifier = Objects.requireNonNull(recordIdentifier, "recordIdentifier must not be null");
        this.entry = entry;
    }

    public static EventRecordChange upsert(EventRecordEntry entry) {
        Objects.requireNonNull(entry, "entry must not be null");
        return new EventRecordChange(Type.UPSERT, entry.getRecordIdentifier(), entry);
    }

    public static EventRecordChange delete(String recordIdentifier) {
        return new EventRecordChange(Type.DELETE, recordIdentifier, null);
    }

    public Type getType() {
        return type;
    }

    public String getRecordIdentifier() {
        return recordIdentifier;
    }

    public EventRecordEntry getEntry() {
        return entry;
    }
}
