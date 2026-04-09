package org.drools.ansible.rulebook.integration.ha.api;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.drools.ansible.rulebook.integration.api.RulesExecutor;
import org.drools.ansible.rulebook.integration.api.io.JsonMapper;
import org.drools.ansible.rulebook.integration.ha.model.EventRecord;
import org.drools.ansible.rulebook.integration.ha.model.EventRecord.RecordType;
import org.drools.ansible.rulebook.integration.ha.model.SessionState;
import org.drools.core.time.impl.PseudoClockScheduler;
import org.kie.api.prototype.PrototypeEventInstance;
import org.kie.api.prototype.PrototypeFactInstance;
import org.kie.api.runtime.rule.Match;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.drools.ansible.rulebook.integration.api.domain.temporal.OnceAbstractTimeConstraint.recreateControlEvent;
import static org.drools.ansible.rulebook.integration.ha.api.HAUtils.normalizeControlEventData;

public abstract class AbstractHAStateManager implements HAStateManager {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractHAStateManager.class);

    protected static final String DROOLS_VERSION_KEY = "drools_version";
    public static final String DROOLS_VERSION = "ha-poc-0.0.8";

    private final Map<String, SessionState> sessionStateMap = new HashMap<>();

    private HAEncryption encryption; // null = disabled

    protected long gracePeriodMs = 0;

    private volatile Thread shutdownHook;

    /**
     * Common initialization for cross-cutting concerns (encryption, future features).
     * Must be called by every subclass at the end of {@code initializeHA()}.
     */
    protected final void commonInit(Map<String, Object> config) {
        initializeEncryption(config);
        initializeGracePeriod(config);
        registerShutdownHook();
    }

    private void registerShutdownHook() {
        if (shutdownHook != null) {
            return; // already registered
        }
        shutdownHook = new Thread(() -> {
            LOG.warn("JVM shutdown hook triggered -- closing HA state manager");
            try {
                shutdown();
            } catch (Exception e) {
                // Best-effort; logging may already be shut down
            }
        }, "ha-state-manager-shutdown");
        Runtime.getRuntime().addShutdownHook(shutdownHook);
        LOG.debug("Registered JVM shutdown hook for HA state manager cleanup");
    }

    /**
     * Remove the shutdown hook during normal shutdown to avoid redundant execution.
     * Subclasses should call this at the beginning of their {@code shutdown()} method.
     */
    protected void deregisterShutdownHook() {
        if (shutdownHook != null) {
            try {
                Runtime.getRuntime().removeShutdownHook(shutdownHook);
            } catch (IllegalStateException e) {
                // JVM is already shutting down -- hook is running or has run
            }
            shutdownHook = null;
        }
    }

    private void initializeGracePeriod(Map<String, Object> config) {
        if (config == null) return;
        Object value = config.get("expired_window_grace_period");
        if (value instanceof Number) {
            long seconds = ((Number) value).longValue();
            if (seconds < 0) {
                seconds = 0;
            }
            this.gracePeriodMs = seconds * 1000L;
            if (this.gracePeriodMs > 0) {
                LOG.info("Expired window grace period set to {} seconds ({}ms)", seconds, this.gracePeriodMs);
            }
        }
    }

    private void initializeEncryption(Map<String, Object> config) {
        if (config == null) return;
        String primaryKey = (String) config.get("encryption_key_primary");
        String secondaryKey = (String) config.get("encryption_key_secondary");
        if (primaryKey != null && !primaryKey.isEmpty()) {
            this.encryption = new HAEncryption(primaryKey, secondaryKey);
            LOG.info("HA encryption enabled (primary key configured, secondary key {})",
                     secondaryKey != null && !secondaryKey.isEmpty() ? "configured" : "not configured");
        } else {
            LOG.info("HA encryption disabled (no encryption_key_primary in config)");
        }
    }

    protected String encryptIfEnabled(String plaintext) {
        if (encryption == null || plaintext == null || plaintext.isEmpty()) return plaintext;
        return encryption.encrypt(plaintext);
    }

    protected String decryptIfEnabled(String data) {
        if (data == null) return data;
        if (encryption == null) {
            if (HAEncryption.isEncrypted(data)) {
                throw new HAEncryptionException(
                        "FATAL: Encrypted data found in database but no encryption keys configured. "
                        + "Provide encryption_key_primary before restarting.");
            }
            return data;
        }
        HAEncryption.DecryptResult result = encryption.decrypt(data);
        if (result.usedSecondaryKey()) {
            LOG.info("Data decrypted with secondary key (will be re-encrypted with primary on next write)");
        }
        return result.plaintext();
    }

    protected void ensureVersionInMetadata(Map<String, Object> metadata) {
        metadata.put(DROOLS_VERSION_KEY, DROOLS_VERSION);
    }

    protected final void validateSessionStateForPersist(SessionState sessionState, boolean isLeader) {
        if (!isLeader) {
            throw new IllegalStateException("Cannot persist SessionState - not leader");
        }
        if (sessionState == null) {
            throw new IllegalArgumentException("SessionState must not be null");
        }
        if (sessionState.getRuleSetName() == null) {
            throw new IllegalArgumentException("SessionState.ruleSetName must be set");
        }
        if (sessionState.getRulebookHash() == null || sessionState.getRulebookHash().isEmpty()) {
            throw new IllegalArgumentException("SessionState.rulebookHash must be set");
        }
        if (sessionState.getCreatedTime() <= 0) {
            throw new IllegalArgumentException("SessionState.createdTime must be > 0");
        }

        String currentStateSHA = sessionState.getCurrentStateSHA();
        if (currentStateSHA == null || currentStateSHA.isEmpty()) {
            throw new IllegalArgumentException("SessionState.currentStateSHA must be set");
        }

        String recalculatedSHA = HAUtils.calculateStateSHA(sessionState);
        if (!currentStateSHA.equals(recalculatedSHA)) {
            throw new IllegalArgumentException(
                    "SessionState.currentStateSHA does not match calculated state SHA");
        }
    }

    @Override
    public RulesExecutor recoverSession(String rulesetString, SessionState sessionState, long currentTimeAtNewNode) {
        return HARulesExecutorFactory.createRulesExecutorWithRecovery(rulesetString, rulesExecutor -> {
            // Replay events to bring session up-to-date
            ((PseudoClockScheduler) rulesExecutor.asKieSession().getSessionClock()).setStartupTime(sessionState.getCreatedTime());
            long currentTime = sessionState.getCreatedTime();
            List<EventRecord> partialEvents = sessionState.getPartialEvents();

            // pre-scan for embedded user events inside CONTROL_TIMED_OUT records to avoid double-replaying them
            Set<Map<String, Object>> embeddedEventMaps = preScanEmbeddedEventsInControlTimedOut(partialEvents);

            // pre-scan for grace-eligible controls to determine which rules may produce matches that are eligible for grace period recovery
            Map<String, Long> ruleExpiryTimes = preScanExpiredGraceEligibleControl(currentTimeAtNewNode, partialEvents);

            // pre-scan for expired accumulate_within controls to log WARN (these expire silently during clock advance)
            logExpiredAccumulateWithinControls(currentTimeAtNewNode, partialEvents);

            // pre-scan for expired time_window sentinel controls to log WARN
            logExpiredTimeWindowControls(currentTimeAtNewNode, partialEvents);

            for (EventRecord eventRecord : partialEvents) {
                rulesExecutor.advanceTime(eventRecord.getInsertedAt() - currentTime, java.util.concurrent.TimeUnit.MILLISECONDS);
                RecordType recordType = eventRecord.getRecordType();
                if (recordType == RecordType.CONTROL_TIMED_OUT) {
                    // TimedOut control events hold a reference to the original user event in their "event" property.
                    // Cleanup rules use `this == $c.event` requiring object identity.
                    // We insert the embedded user event into WM first, then the control event.
                    // The control's "event" property already points to the same PrototypeFactInstance object.
                    Map<String, Object> eventData = normalizeControlEventData(JsonMapper.readValueAsMapOfStringAndObject(eventRecord.getEventJson()));
                    PrototypeEventInstance controlEvent = recreateControlEvent(eventData, eventRecord.getExpirationDuration());
                    Object embeddedEvent = controlEvent.get("event");
                    if (embeddedEvent instanceof PrototypeFactInstance) {
                        rulesExecutor.asKieSession().insert(embeddedEvent);
                        LOG.debug("  * Inserted embedded user event for CONTROL_TIMED_OUT at time {}", eventRecord.getInsertedAt());
                    }
                    rulesExecutor.asKieSession().insert(controlEvent);
                    if (eventRecord.getExpirationDuration() != Long.MAX_VALUE) {
                        LOG.debug("  * Recovered CONTROL_TIMED_OUT at time {}, expiration duration: {} ms : {}", eventRecord.getInsertedAt(), eventRecord.getExpirationDuration(), controlEvent);
                    } else {
                        LOG.debug("  * Recovered CONTROL_TIMED_OUT at time {}, no expiration : {}", eventRecord.getInsertedAt(), controlEvent);
                    }
                } else if (recordType.isSynthetic()) {
                    // How time constraints are handled??
                    // OnceWithin : If a control event exists (= not yet expired), an event is discarded without firing the rule. [on recover]-> Inserting the control event back is sufficient.
                    // AggregateWithin : A control event holds the number of events. Events are discarded until the threshold is met. [on recover] -> Inserting the control event back is sufficient.
                    // TimeWindow : Sentinel control events per pattern (HA-only). [on recover] -> Insert sentinels back; pre-scan logs WARN for expired ones.
                    // OnceAfter : A main control event holds nested events (main control event may be multiple because of group_by).
                    //             When its start control event expires and its end control event is present, the rule fires. [on recover] -> Inserting the all control events back is sufficient.
                    Map<String, Object> eventData = normalizeControlEventData(JsonMapper.readValueAsMapOfStringAndObject(eventRecord.getEventJson()));
                    PrototypeEventInstance controlEvent = recreateControlEvent(eventData, eventRecord.getExpirationDuration());
                    rulesExecutor.asKieSession().insert(controlEvent);
                    if (eventRecord.getExpirationDuration() != Long.MAX_VALUE) {
                        LOG.debug("  * Recovered control event at time {}, expiration duration: {} ms : {}", eventRecord.getInsertedAt(), eventRecord.getExpirationDuration(), controlEvent);
                    } else {
                        LOG.debug("  * Recovered control event at time {}, no expiration : {}", eventRecord.getInsertedAt(), controlEvent);
                    }
                } else if (recordType == RecordType.FACT) {
                    LOG.debug("  * Replaying fact event at time {}: {}", eventRecord.getInsertedAt(), eventRecord.getEventJson());
                    rulesExecutor.processFacts(eventRecord.getEventJson());
                } else {
                    // RecordType.EVENT — skip if already embedded in a CONTROL_TIMED_OUT record
                    if (!embeddedEventMaps.isEmpty()) {
                        Map<String, Object> eventMap = JsonMapper.readValueAsMapOfStringAndObject(eventRecord.getEventJson());
                        if (embeddedEventMaps.contains(eventMap)) {
                            LOG.debug("  * Skipping EVENT at time {} (already embedded in CONTROL_TIMED_OUT): {}", eventRecord.getInsertedAt(), eventRecord.getEventJson());
                            currentTime = eventRecord.getInsertedAt();
                            continue;
                        }
                    }
                    LOG.debug("  * Replaying event at time {}: {}", eventRecord.getInsertedAt(), eventRecord.getEventJson());
                    rulesExecutor.processEvents(eventRecord.getEventJson());
                }
                currentTime = eventRecord.getInsertedAt();
            }

            // Advance clock to persisted time and then to current node time, capturing matches
            List<Match> catchUpMatches = rulesExecutor.advanceTime(sessionState.getPersistedTime() - currentTime, java.util.concurrent.TimeUnit.MILLISECONDS).join();
            List<Match> nodeTimeMatches = Collections.emptyList();
            // TODO: Do we need to consider clock drift between nodes?
            if (currentTimeAtNewNode > sessionState.getPersistedTime()) {
                LOG.debug("  Advancing recovered session clock from persisted time to current node time");
                nodeTimeMatches = rulesExecutor.advanceTime(currentTimeAtNewNode - sessionState.getPersistedTime(), java.util.concurrent.TimeUnit.MILLISECONDS).join();
            }

            // Restore processed event IDs from persisted state for duplicate detection
            if (sessionState.getProcessedEventIds() != null) {
                rulesExecutor.getHaSessionContext().setProcessedEventIds(sessionState.getProcessedEventIds());
                LOG.debug("  Restored {} processed event IDs from persisted state", sessionState.getProcessedEventIds().size());
            }

            // Filter recovery matches: log WARN for dropped matches, store eligible ones (within grace period) on the executor
            List<Match> allRecoveryMatches = new ArrayList<>(catchUpMatches);
            allRecoveryMatches.addAll(nodeTimeMatches);
            filterAndStoreGracePeriodMatches(allRecoveryMatches, ruleExpiryTimes, currentTimeAtNewNode, rulesExecutor);
        });
    }

    private Map<String, Long> preScanExpiredGraceEligibleControl(long currentTimeAtNewNode, List<EventRecord> partialEvents) {
        // Pre-scan: build map of rule name → window expiry time
        // for grace-eligible controls (CONTROL_ONCE_AFTER and CONTROL_TIMED_OUT).
        // This runs unconditionally so expired windows are always detected and logged,
        // even when gracePeriodMs = 0 (all expired matches will be WARN-logged and dropped).
        Map<String, Long> ruleExpiryTimes = new HashMap<>();
        for (EventRecord er : partialEvents) {
            if (isGracePeriodEligible(er)) {
                long expiryTime = er.getInsertedAt() + er.getExpirationDuration();
                if (expiryTime <= currentTimeAtNewNode) { // already expired
                    String ruleName = extractUserRuleNameFromControl(er);
                    if (ruleName != null) {
                        ruleExpiryTimes.merge(ruleName, expiryTime, Math::max);
                    }
                }
            }
        }
        if (!ruleExpiryTimes.isEmpty()) {
            LOG.info("Recovery pre-scan found {} rules with expired windows: {}", ruleExpiryTimes.size(), ruleExpiryTimes.keySet());
        }
        return ruleExpiryTimes;
    }

    private void logExpiredAccumulateWithinControls(long currentTimeAtNewNode, List<EventRecord> partialEvents) {
        for (EventRecord er : partialEvents) {
            if (er.getRecordType() == RecordType.CONTROL_ACCUMULATE_WITHIN
                    && er.getExpirationDuration() != null && er.getExpirationDuration() != Long.MAX_VALUE) {
                long expiryTime = er.getInsertedAt() + er.getExpirationDuration();
                if (expiryTime <= currentTimeAtNewNode) {
                    try {
                        Map<String, Object> data = JsonMapper.readValueAsMapOfStringAndObject(er.getEventJson());
                        String ruleName = data.get("drools_rule_name") instanceof String s ? s : "unknown";
                        Object count = data.get("current_count");
                        long expiredAgo = currentTimeAtNewNode - expiryTime;
                        LOG.warn("accumulate_within window expired during outage for rule '{}' (accumulated count={}, window={}ms, expired {}ms ago)",
                                ruleName, count, er.getExpirationDuration(), expiredAgo);
                    } catch (Exception e) {
                        LOG.warn("accumulate_within window expired during outage (failed to parse control event details: {})", e.getMessage());
                    }
                }
            }
        }
    }

    private void logExpiredTimeWindowControls(long currentTimeAtNewNode, List<EventRecord> partialEvents) {
        // Group expired CONTROL_TIME_WINDOW sentinels by rule name to produce a single WARN per rule
        Map<String, List<EventRecord>> expiredByRule = new HashMap<>();
        for (EventRecord er : partialEvents) {
            if (er.getRecordType() == RecordType.CONTROL_TIME_WINDOW
                    && er.getExpirationDuration() != null && er.getExpirationDuration() != Long.MAX_VALUE) {
                long expiryTime = er.getInsertedAt() + er.getExpirationDuration();
                if (expiryTime <= currentTimeAtNewNode) {
                    try {
                        Map<String, Object> data = JsonMapper.readValueAsMapOfStringAndObject(er.getEventJson());
                        String ruleName = data.get("drools_rule_name") instanceof String s ? s : "unknown";
                        expiredByRule.computeIfAbsent(ruleName, k -> new ArrayList<>()).add(er);
                    } catch (Exception e) {
                        LOG.warn("all+timeout window sentinel expired during outage (failed to parse details: {})", e.getMessage());
                    }
                }
            }
        }

        for (Map.Entry<String, List<EventRecord>> entry : expiredByRule.entrySet()) {
            String ruleName = entry.getKey();
            List<EventRecord> sentinels = entry.getValue();

            int totalPatterns = 0;
            long windowMs = 0;
            long latestExpiry = 0;
            List<Integer> matchedPatternIndices = new ArrayList<>();
            List<Object> matchedEvents = new ArrayList<>();

            for (EventRecord sentinel : sentinels) {
                try {
                    Map<String, Object> data = JsonMapper.readValueAsMapOfStringAndObject(sentinel.getEventJson());
                    if (data.get("total_patterns") instanceof Number n) totalPatterns = n.intValue();
                    if (data.get("pattern_index") instanceof Number n) matchedPatternIndices.add(n.intValue());
                    // Each sentinel stores the event that triggered it
                    Object matchedEvent = data.get("matched_event");
                    if (matchedEvent != null) {
                        matchedEvents.add(matchedEvent);
                    }
                } catch (Exception ignored) { }
                windowMs = sentinel.getExpirationDuration();
                latestExpiry = Math.max(latestExpiry, sentinel.getInsertedAt() + sentinel.getExpirationDuration());
            }

            long expiredAgo = currentTimeAtNewNode - latestExpiry;

            LOG.warn("all+timeout window expired during outage for rule '{}' (matched_patterns={}/{}, window={}ms, expired {}ms ago, matched_events={})",
                    ruleName, matchedPatternIndices.size(), totalPatterns, windowMs, expiredAgo, matchedEvents);
        }
    }

    private static Set<Map<String, Object>> preScanEmbeddedEventsInControlTimedOut(List<EventRecord> partialEvents) {
        // Pre-scan: collect EVENT maps that are embedded inside CONTROL_TIMED_OUT records.
        // These user events must NOT be replayed via processEvents() (which would re-trigger pattern rules).
        // Instead they will be inserted directly into WM when their parent control is recovered.
        Set<Map<String, Object>> embeddedEventMaps = new HashSet<>();
        for (EventRecord er : partialEvents) {
            if (er.getRecordType() == RecordType.CONTROL_TIMED_OUT) {
                Map<String, Object> controlData = JsonMapper.readValueAsMapOfStringAndObject(er.getEventJson());
                Object embeddedEvent = controlData.get("event");
                if (embeddedEvent instanceof Map) {
                    embeddedEventMaps.add((Map<String, Object>) embeddedEvent);
                }
            }
        }
        return embeddedEventMaps;
    }

    /**
     * Filter recovery matches by grace period eligibility and store eligible ones on the executor.
     */
    private void filterAndStoreGracePeriodMatches(List<Match> recoveryMatches, Map<String, Long> ruleExpiryTimes,
                                                  long currentTimeAtNewNode, HARulesExecutor haExecutor) {
        if (recoveryMatches.isEmpty()) return;

        List<Match> eligibleMatches = new ArrayList<>();
        for (Match match : recoveryMatches) {
            String matchRuleName = match.getRule().getName();
            Long expiryTime = ruleExpiryTimes.get(matchRuleName);
            if (expiryTime == null) {
                LOG.debug("Skipping recovery match for rule '{}' (not found in grace period pre-scan)", matchRuleName);
                continue;
            }
            long lateness = currentTimeAtNewNode - expiryTime;

            if (lateness <= gracePeriodMs) {
                LOG.info("Recovery match within grace period for rule '{}' (expired {}ms ago, grace={}ms)",
                        matchRuleName, lateness, gracePeriodMs);
                eligibleMatches.add(match);
            } else {
                LOG.warn("Dropping expired recovery match for rule '{}' (expired {}ms ago, grace={}ms), events={}",
                        matchRuleName, lateness, gracePeriodMs, match.getObjects());
            }
        }

        if (!eligibleMatches.isEmpty()) {
            haExecutor.addRecoveryMatches(eligibleMatches);
        }
    }

    /**
     * Check if an event record is eligible for grace period recovery.
     * Only once_after and timed_out (not_all+timeout) controls with finite expiration are grace-eligible.
     */
    private static boolean isGracePeriodEligible(EventRecord er) {
        if (er.getExpirationDuration() == null || er.getExpirationDuration() == Long.MAX_VALUE) return false;
        return er.getRecordType() == RecordType.CONTROL_ONCE_AFTER || er.getRecordType() == RecordType.CONTROL_TIMED_OUT;
    }

    /**
     * Extract the user rule name from a grace-eligible control event record.
     * For CONTROL_ONCE_AFTER: looks for "start_once_after" field in the event JSON.
     * For CONTROL_TIMED_OUT: looks for "rulename" field with "start_" prefix.
     * Returns null if the rule name cannot be extracted (safe fallback — event is skipped).
     */
    private String extractUserRuleNameFromControl(EventRecord er) {
        try {
            Map<String, Object> data = JsonMapper.readValueAsMapOfStringAndObject(er.getEventJson());
            if (er.getRecordType() == RecordType.CONTROL_ONCE_AFTER) {
                Object val = data.get("start_once_after");
                return val instanceof String ? (String) val : null;
            } else if (er.getRecordType() == RecordType.CONTROL_TIMED_OUT) {
                Object val = data.get("rulename");
                if (val instanceof String rn && rn.startsWith("start_")) {
                    return rn.substring("start_".length());
                }
            }
        } catch (Exception e) {
            LOG.debug("Failed to extract rule name from control event: {}", e.getMessage());
        }
        return null;
    }

    @Override
    public void registerSessionState(String ruleSetName, SessionState sessionState) {
        sessionStateMap.put(ruleSetName, sessionState);
    }

    @Override
    public SessionState getInMemorySessionState(String ruleSetName) {
        return sessionStateMap.get(ruleSetName);
    }

    @Override
    public void unregisterSessionState(String ruleSetName) {
        sessionStateMap.remove(ruleSetName);
    }

    /**
     * Counts the total number of partial events stored in memory across all session (= rule set) states.
     */
    protected int countPartialEventsInMemory() {
        return sessionStateMap.values().stream()
                .mapToInt(state -> {
                    List<EventRecord> partialEvents = state.getPartialEvents();
                    return partialEvents == null ? 0 : partialEvents.size();
                })
                .sum();
    }

    @Override
    public boolean verifySessionState(SessionState sessionState) {
        if (sessionState == null) {
            return false;
        }

        String storedSHA = sessionState.getCurrentStateSHA();
        if (storedSHA == null) {
            LOG.error("SessionState integrity check FAILED! Missing SHA for {}", sessionState.getRuleSetName());
            throw new IllegalStateException("SessionState integrity check failed: missing SHA for "
                    + sessionState.getRuleSetName());
        }

        // Recalculate SHA from content
        String recalculatedSHA = HAUtils.calculateStateSHA(sessionState);

        boolean valid = storedSHA.equals(recalculatedSHA);

        if (!valid) {
            LOG.error("SessionState integrity check FAILED! Stored SHA: {}, Recalculated SHA: {}",
                      storedSHA, recalculatedSHA);
            LOG.error("SessionState may be corrupted or tampered. RuleSetName: {}, HaUuid: {}",
                      sessionState.getRuleSetName(), sessionState.getHaUuid());
            throw new IllegalStateException("SessionState integrity check failed for " + sessionState.getRuleSetName());
        } else {
            LOG.debug("SessionState integrity check passed for {}", sessionState.getRuleSetName());
        }

        return valid;
    }
}
