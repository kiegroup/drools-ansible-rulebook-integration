package org.drools.ansible.rulebook.integration.ha.api;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Comparator;

import org.drools.ansible.rulebook.integration.api.rulesmodel.RulesModelUtil;
import org.drools.ansible.rulebook.integration.api.io.JsonMapper;
import org.drools.ansible.rulebook.integration.ha.model.EventRecord;
import org.drools.ansible.rulebook.integration.ha.model.EventRecordEntry;
import org.drools.ansible.rulebook.integration.ha.model.SessionState;
import org.drools.model.prototype.impl.HashMapEventImpl;
import org.kie.api.prototype.PrototypeFactInstance;
import org.kie.api.runtime.rule.FactHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HAUtils {

    private static final Logger LOG = LoggerFactory.getLogger(HAUtils.class);

    private HAUtils() {}

    public static Optional<String> getEventUuid(FactHandle handle) {
        Object object = handle.getObject();
        if (object instanceof HashMapEventImpl hashMapEvent) {
            return getEventUuid(hashMapEvent.asMap());
        }
        return Optional.empty();
    }

    // ansible-rulebook event always has meta -> uuid
    public static Optional<String> getEventUuid(Map<String, Object> event) {
        if (event.get("meta") instanceof Map<?,?> metaMap && metaMap.get("uuid") instanceof String eventUuid) {
            return Optional.of(eventUuid);
        }
        return Optional.empty();
    }

    private static final HexFormat HEX = HexFormat.of();

    public static byte[] sha256(byte[] data) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            return md.digest(data);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static String sha256(String s) {
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        byte[] hash = md.digest(s.getBytes(StandardCharsets.UTF_8));
        return HEX.formatHex(hash);
    }

    /**
     * Calculate SHA256 of the SessionState content for integrity verification.
     * This can detect corruption or tampering of persisted data.
     *
     * @param sessionState The session state to hash
     * @return SHA256 hex string of the state content
     */
    public static String calculateStateSHA(SessionState sessionState) {
        if (sessionState == null) {
            return null;
        }

        String hashableContent = sessionState.toHashableContent();
        return sha256(hashableContent);
    }

    public static String calculateEventRecordSHA(EventRecordEntry entry) {
        if (entry == null || entry.getRecord() == null) {
            return null;
        }
        return sha256(toEventRecordHashableContent(entry));
    }

    public static String calculateEventRecordsManifestSHA(List<EventRecordEntry> entries) {
        if (entries == null) {
            return sha256("[]");
        }
        List<Map<String, Object>> manifest = entries.stream()
                .sorted(Comparator.comparingLong(EventRecordEntry::getRecordSequence)
                                   .thenComparing(EventRecordEntry::getRecordIdentifier))
                .map(entry -> {
                    Map<String, Object> contentMap = new LinkedHashMap<>();
                    contentMap.put("recordIdentifier", entry.getRecordIdentifier());
                    contentMap.put("recordSequence", entry.getRecordSequence());
                    contentMap.put("eventRecordSHA", entry.getEventRecordSHA() != null
                            ? entry.getEventRecordSHA()
                            : calculateEventRecordSHA(entry));
                    return contentMap;
                })
                .toList();
        return sha256(JsonMapper.toJson(manifest));
    }

    private static String toEventRecordHashableContent(EventRecordEntry entry) {
        EventRecord record = entry.getRecord();
        Map<String, Object> contentMap = new LinkedHashMap<>();
        contentMap.put("recordIdentifier", entry.getRecordIdentifier());
        contentMap.put("recordSequence", entry.getRecordSequence());
        contentMap.put("insertedAt", record.getInsertedAt());
        contentMap.put("recordType", record.getRecordType());
        contentMap.put("eventJson", record.getEventJson());
        contentMap.put("expirationDuration", record.getExpirationDuration());
        return JsonMapper.toJson(contentMap);
    }

    /**
     * Populate a HA match response as new scheme. We may apply this to non-HA use case in the future.
     * @param matchResponse
     * @param ruleName
     * @param events
     * @param meUuid
     */
    public static void populateHAMatchResponse(Map<String, Object> matchResponse, String ruleName, Map<String, Object> events, String meUuid) {
        matchResponse.put("name", ruleName);
        matchResponse.put("events", events);
        matchResponse.put("matching_uuid", meUuid);
    }

    /**
     * Flatten PrototypeFactInstance to a plain Map, converting nested PrototypeFactInstances as well.
     * Avoids Jackson serializing PrototypeEventInstance internals (prototype metadata) instead of payload.
     */
    public static Map<String, Object> flattenPrototypeFact(PrototypeFactInstance fact) {
        Map<String, Object> raw = fact.asMap();
        Map<String, Object> result = new HashMap<>(raw.size());
        for (Map.Entry<String, Object> entry : raw.entrySet()) {
            Object value = entry.getValue();
            if (value instanceof PrototypeFactInstance nested) {
                result.put(entry.getKey(), flattenPrototypeFact(nested));
            } else {
                result.put(entry.getKey(), value);
            }
        }
        return result;
    }

    /**
     * Convert nested event maps back into PrototypeEventInstance so control facts retain the original event payload.
     */
    public static Map<String, Object> normalizeControlEventData(Map<String, Object> raw) {
        if (raw == null || raw.isEmpty()) {
            return raw;
        }
        Map<String, Object> normalized = new HashMap<>(raw.size());
        raw.forEach((k, v) -> {
            if ("event".equals(k) && v instanceof Map<?, ?> nested) {
                normalized.put(k, RulesModelUtil.mapToFact((Map<String, Object>) nested, true));
            } else {
                normalized.put(k, v);
            }
        });
        return normalized;
    }
}
