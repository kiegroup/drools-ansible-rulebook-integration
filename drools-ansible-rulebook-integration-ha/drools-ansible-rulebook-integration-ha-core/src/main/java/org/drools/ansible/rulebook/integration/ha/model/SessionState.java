package org.drools.ansible.rulebook.integration.ha.model;

import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.drools.ansible.rulebook.integration.api.io.JsonMapper;

/**
 * Represents the state of events in the HA system.
 * Contains both regular event processing state and in-flight matching events.
 */
public class SessionState {

    private String haUuid;
    private String ruleSetName;
    private String rulebookHash;

    private List<EventRecord> partialEvents;

    private List<String> processedEventIds;

    private long createdTime;
    private long persistedTime;

    // Metadata
    private String leaderId;

    // For integrity checks
    private String currentStateSHA;      // SHA256 of current state

    // Extensibility columns for future use without schema migration
    private Map<String, Object> metadata = new HashMap<>();
    private Map<String, Object> properties = new HashMap<>();
    private Map<String, Object> settings = new HashMap<>();
    private Map<String, Object> ext = new HashMap<>();

    public SessionState() {
        this.createdTime = Instant.now().toEpochMilli();
    }

    public String getHaUuid() {
        return haUuid;
    }

    public void setHaUuid(String haUuid) {
        this.haUuid = haUuid;
    }

    public String getRulebookHash() {
        return rulebookHash;
    }

    public void setRulebookHash(String rulebookHash) {
        this.rulebookHash = rulebookHash;
    }

    public String getRuleSetName() {
        return ruleSetName;
    }

    public void setRuleSetName(String ruleSetName) {
        this.ruleSetName = ruleSetName;
    }

    public List<EventRecord> getPartialEvents() {
        return partialEvents;
    }

    public void setPartialEvents(List<EventRecord> partialEvents) {
        this.partialEvents = partialEvents;
    }

    public List<String> getProcessedEventIds() {
        return processedEventIds;
    }

    public void setProcessedEventIds(List<String> processedEventIds) {
        this.processedEventIds = processedEventIds;
    }

    public long getPersistedTime() {
        return persistedTime;
    }

    public void setPersistedTime(long persistedTime) {
        this.persistedTime = persistedTime;
    }

    public long getCreatedTime() {
        return createdTime;
    }

    public void setCreatedTime(long createdTime) {
        this.createdTime = createdTime;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public void setLeaderId(String leaderId) {
        this.leaderId = leaderId;
    }

    public String getCurrentStateSHA() {
        return currentStateSHA;
    }

    public void setCurrentStateSHA(String currentStateSHA) {
        this.currentStateSHA = currentStateSHA;
    }


    public Map<String, Object> getMetadata() {
        return metadata;
    }

    public void setMetadata(Map<String, Object> metadata) {
        this.metadata = metadata != null ? metadata : new HashMap<>();
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, Object> properties) {
        this.properties = properties != null ? properties : new HashMap<>();
    }

    public Map<String, Object> getSettings() {
        return settings;
    }

    public void setSettings(Map<String, Object> settings) {
        this.settings = settings != null ? settings : new HashMap<>();
    }

    public Map<String, Object> getExt() {
        return ext;
    }

    public void setExt(Map<String, Object> ext) {
        this.ext = ext != null ? ext : new HashMap<>();
    }

    /**
     * Returns a canonical representation of this SessionState for SHA calculation.
     * Excludes fields that are not part of the semantic working memory state:
     * - currentStateSHA: can't hash itself (circular dependency)
     * - metadata, properties, settings, ext: extensibility placeholders, not semantic state
     *
     * @return JSON string with deterministic field ordering for consistent hashing
     */
    public String toHashableContent() {
        // Create a map with fields that represent working memory state
        Map<String, Object> contentMap = new LinkedHashMap<>();

        contentMap.put("haUuid", haUuid);
        contentMap.put("ruleSetName", ruleSetName);
        contentMap.put("rulebookHash", rulebookHash);
        // Keep partialEvents as typed EventRecord objects. Do not parse eventJson into
        // Map<String, Object> or re-serialize it here; JSON numeric type drift or map
        // ordering changes could otherwise create false SHA mismatches after recovery.
        contentMap.put("partialEvents", partialEvents);
        contentMap.put("processedEventIds", processedEventIds);
        contentMap.put("createdTime", createdTime);
        contentMap.put("persistedTime", persistedTime);
        contentMap.put("leaderId", leaderId);

        return JsonMapper.toJson(contentMap);
    }
}
