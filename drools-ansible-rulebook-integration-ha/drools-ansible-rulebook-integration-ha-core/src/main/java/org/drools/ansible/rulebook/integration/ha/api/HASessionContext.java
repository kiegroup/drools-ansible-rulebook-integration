package org.drools.ansible.rulebook.integration.ha.api;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import org.drools.ansible.rulebook.integration.ha.model.EventRecord;
import org.drools.ansible.rulebook.integration.ha.model.EventRecordChange;
import org.drools.ansible.rulebook.integration.ha.model.EventRecordEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HASessionContext {

    private static final Logger logger = LoggerFactory.getLogger(HASessionContext.class);

    // No concurrency expected, AbstractRulesEvaluator.atomicRuleEvaluation ensures synchronized access

    // Track events/facts existing in the session. Eventually persisted as SessionState.partialEvents
    private final LinkedHashMap<String, EventRecord> trackedRecords = new LinkedHashMap<>();

    // Associate factHandleIds to identifiers for efficient lookup during updates/deletions
    private final Map<Long, String> factHandleIndex = new HashMap<>();

    // Stable per-record sequence for deterministic row-backed recovery ordering
    private final Map<String, Long> recordSequenceIndex = new HashMap<>();

    private final Map<String, String> eventRecordShaIndex = new HashMap<>();

    private final List<EventRecordChange> pendingEventRecordChanges = new ArrayList<>();

    private long nextRecordSequence = 0L;

    // Circular buffer of processed event IDs for duplicate detection
    private final LinkedHashSet<String> processedEventIds = new LinkedHashSet<>();
    private static final int DEFAULT_MAX_PROCESSED_IDS = 5;
    private int maxProcessedIds = DEFAULT_MAX_PROCESSED_IDS;

    // Temporarily hold incoming event/fact metadata during insertion flow.
    // Preserves original JSON and identifier from client before Drools processing.
    // Used to distinguish user events/facts from synthetic control events.
    private PendingRecord pendingRecord;

    public LinkedHashMap<String, EventRecord> getTrackedRecords() {
        return trackedRecords;
    }

    public void addTrackedRecord(String identifier, EventRecord eventRecord, Long factHandleId) {
        if (identifier == null || eventRecord == null) {
            return;
        }

        removeFactHandleIndexForIdentifier(identifier);
        trackedRecords.put(identifier, eventRecord);
        long recordSequence = recordSequenceIndex.computeIfAbsent(identifier, ignored -> nextRecordSequence++);
        EventRecordEntry entry = new EventRecordEntry(identifier, recordSequence, eventRecord);
        String eventRecordSHA = HAUtils.calculateEventRecordSHA(entry);
        eventRecordShaIndex.put(identifier, eventRecordSHA);
        pendingEventRecordChanges.add(EventRecordChange.upsert(new EventRecordEntry(identifier, recordSequence, eventRecord, eventRecordSHA)));
        if (factHandleId != null) {
            factHandleIndex.put(factHandleId, identifier);
        }
    }

    public void removeTrackedRecord(String identifier) {
        if (identifier == null) {
            return;
        }
        if (trackedRecords.remove(identifier) != null) {
            removeFactHandleIndexForIdentifier(identifier);
            recordSequenceIndex.remove(identifier);
            eventRecordShaIndex.remove(identifier);
            pendingEventRecordChanges.add(EventRecordChange.delete(identifier));
        }
    }

    /**
     * Discards a record that was inserted and then immediately dropped during the same
     * evaluation cycle before it ever became retained HA state.
     *
     * This is used for unmatched/disconnected facts/events. In that case we must not
     * persist an UPSERT followed by a DELETE because some backends, notably H2 with
     * large CLOB payloads, still materialize the transient row contents in memory.
     */
    public void discardTrackedRecord(String identifier) {
        if (identifier == null) {
            return;
        }
        if (trackedRecords.remove(identifier) != null) {
            removeFactHandleIndexForIdentifier(identifier);
            recordSequenceIndex.remove(identifier);
            eventRecordShaIndex.remove(identifier);
            pendingEventRecordChanges.removeIf(change -> identifier.equals(change.getRecordIdentifier()));
        }
    }

    public void removeTrackedRecordByFactHandle(long factHandleId) {
        String identifier = factHandleIndex.remove(factHandleId);
        if (identifier != null) {
            removeTrackedRecord(identifier);
        }
    }

    public void discardTrackedRecordByFactHandle(long factHandleId) {
        String identifier = factHandleIndex.remove(factHandleId);
        if (identifier != null) {
            discardTrackedRecord(identifier);
        }
    }

    /**
     * Updates an existing EventRecord's JSON content.
     * Used when a control event is modified (e.g., AccumulateWithin increments current_count).
     *
     * @param factHandleId The fact handle ID of the object being updated
     * @param updatedJson The new JSON representation of the object
     */
    public void updateTrackedRecordByFactHandle(long factHandleId, String updatedJson) {
        String identifier = factHandleIndex.get(factHandleId);
        if (identifier != null) {
            EventRecord eventRecord = trackedRecords.get(identifier);
            if (eventRecord != null) {
                eventRecord.setEventJson(updatedJson);
                long recordSequence = recordSequenceIndex.computeIfAbsent(identifier, ignored -> nextRecordSequence++);
                EventRecordEntry entry = new EventRecordEntry(identifier, recordSequence, eventRecord);
                String eventRecordSHA = HAUtils.calculateEventRecordSHA(entry);
                eventRecordShaIndex.put(identifier, eventRecordSHA);
                pendingEventRecordChanges.add(EventRecordChange.upsert(new EventRecordEntry(identifier, recordSequence, eventRecord, eventRecordSHA)));
                logger.debug("Updated EventRecord for identifier: {}, factHandleId: {}", identifier, factHandleId);
            } else {
                logger.warn("No EventRecord found for identifier: {} during update", identifier);
            }
        } else {
            logger.warn("No identifier mapping found for factHandleId: {} during update", factHandleId);
        }
    }

    public void preparePendingRecord(String identifier, String json, EventRecord.RecordType type) {
        if (identifier == null || json == null || type == null) {
            throw new IllegalArgumentException("Invalid arguments passed to preparePendingRecord : " +
                                                       "identifier=" + identifier + ", json=" + json + ", type=" + type);
        }
        pendingRecord = new PendingRecord(identifier, json, type);
    }

    public PendingRecord consumePendingRecord() {
        PendingRecord theRecord = pendingRecord;
        pendingRecord = null;
        return theRecord;
    }

    public void setMaxProcessedIds(int maxProcessedIds) {
        this.maxProcessedIds = maxProcessedIds;
    }

    public boolean isAlreadyProcessed(String eventId) {
        return processedEventIds.contains(eventId);
    }

    public void recordProcessedEvent(String eventId) {
        processedEventIds.add(eventId);
        if (processedEventIds.size() > maxProcessedIds) {
            Iterator<String> it = processedEventIds.iterator();
            it.next();
            it.remove();
        }
    }

    public List<String> getProcessedEventIds() {
        return new ArrayList<>(processedEventIds);
    }

    public void setProcessedEventIds(List<String> ids) {
        processedEventIds.clear();
        if (ids != null) {
            processedEventIds.addAll(ids);
        }
    }

    public List<EventRecordChange> drainEventRecordChanges() {
        List<EventRecordChange> changes = new ArrayList<>(pendingEventRecordChanges);
        pendingEventRecordChanges.clear();
        return changes;
    }

    public List<EventRecordEntry> snapshotEventRecordEntries() {
        List<EventRecordEntry> entries = new ArrayList<>(trackedRecords.size());
        for (Map.Entry<String, EventRecord> entry : trackedRecords.entrySet()) {
            String identifier = entry.getKey();
            long recordSequence = recordSequenceIndex.computeIfAbsent(identifier, ignored -> nextRecordSequence++);
            String eventRecordSHA = eventRecordShaIndex.computeIfAbsent(identifier,
                    ignored -> HAUtils.calculateEventRecordSHA(new EventRecordEntry(identifier, recordSequence, entry.getValue())));
            entries.add(new EventRecordEntry(identifier, recordSequence, entry.getValue(), eventRecordSHA));
        }
        return entries;
    }

    private void removeFactHandleIndexForIdentifier(String identifier) {
        factHandleIndex.entrySet().removeIf(entry -> identifier.equals(entry.getValue()));
    }

    public static final class PendingRecord {
        private final String identifier;
        private final String json;
        private final EventRecord.RecordType type;

        PendingRecord(String identifier, String json, EventRecord.RecordType type) {
            this.identifier = identifier;
            this.json = json;
            this.type = type;
        }

        public String getIdentifier() {
            return identifier;
        }

        public String getJson() {
            return json;
        }

        public EventRecord.RecordType getType() {
            return type;
        }
    }
}
