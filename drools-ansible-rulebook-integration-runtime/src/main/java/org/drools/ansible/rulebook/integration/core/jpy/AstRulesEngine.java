package org.drools.ansible.rulebook.integration.core.jpy;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.drools.ansible.rulebook.integration.api.RuleConfigurationOption;
import org.drools.ansible.rulebook.integration.api.RuleFormat;
import org.drools.ansible.rulebook.integration.api.RuleNotation;
import org.drools.ansible.rulebook.integration.api.RulesExecutor;
import org.drools.ansible.rulebook.integration.api.RulesExecutorContainer;
import org.drools.ansible.rulebook.integration.api.RulesExecutorFactory;
import org.drools.ansible.rulebook.integration.api.domain.RuleMatch;
import org.drools.ansible.rulebook.integration.api.domain.RulesSet;
import org.drools.ansible.rulebook.integration.api.io.Response;
import org.drools.ansible.rulebook.integration.api.rulesmodel.RulesModelUtil;
import org.drools.ansible.rulebook.integration.ha.api.HARulesExecutor;
import org.drools.ansible.rulebook.integration.ha.api.HARulesExecutorFactory;
import org.drools.ansible.rulebook.integration.ha.api.HASessionContext;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManager;
import org.drools.ansible.rulebook.integration.ha.api.HAStateManagerFactory;
import org.drools.ansible.rulebook.integration.ha.model.EventRecord;
import org.drools.ansible.rulebook.integration.ha.model.HAStats;
import org.drools.ansible.rulebook.integration.ha.model.MatchingEvent;
import org.drools.ansible.rulebook.integration.ha.model.SessionState;
import org.drools.ansible.rulebook.integration.ha.util.PartialMatchCounter;
import org.drools.ansible.rulebook.integration.api.rulesengine.SessionStats;
import org.kie.api.runtime.rule.Match;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.readValueAsMapOfStringAndObject;
import static org.drools.ansible.rulebook.integration.api.io.JsonMapper.toJson;
import static org.drools.ansible.rulebook.integration.ha.api.HAUtils.calculateStateSHA;
import static org.drools.ansible.rulebook.integration.ha.api.HAUtils.populateHAMatchResponse;
import static org.drools.ansible.rulebook.integration.ha.api.HAUtils.sha256;

public class AstRulesEngine implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(AstRulesEngine.class);
    private static final String EMPTY_ME_UUID = "";

    private final RulesExecutorContainer rulesExecutorContainer = new RulesExecutorContainer();
    private final Map<String, SessionStats> lastAggregatedSessionStatsByLeader = new ConcurrentHashMap<>();
    
    private HAStateManager haStateManager;
    private boolean haMode = false;
    private boolean shutdown = false;
    private int dedupBufferSize = 5;
    private boolean overwriteIfRulebookChanges = true;

    public long createRuleset(String rulesetString) {
        RulesSet rulesSet = RuleNotation.CoreNotation.INSTANCE.toRulesSet(RuleFormat.JSON, rulesetString);
        return createRuleset(rulesSet, rulesetString);
    }

    // for test convenience
    public long createRuleset(String rulesetString, RuleConfigurationOption... options) {
        RulesSet rulesSet = RuleNotation.CoreNotation.INSTANCE.withOptions(options).toRulesSet(RuleFormat.JSON, rulesetString);
        return createRuleset(rulesSet, rulesetString);
    }

    public long createRuleset(RulesSet rulesSet) {
        return createRuleset(rulesSet, null);
    }

    public long createRuleset(RulesSet rulesSet, String rulesetString) {
        checkAlive();
        rulesSet.setHaMode(haMode);
        if (rulesSet.hasTemporalConstraint()) {
            rulesSet.withOptions(RuleConfigurationOption.USE_PSEUDO_CLOCK);
            if (rulesSet.hasAsyncExecution()) {
                rulesExecutorContainer.allowAsync();
            }
        }

        RulesExecutor executor;

        if (haMode && haStateManager != null) {
            // regardless of leader or not, try to restore session state if exists
            executor = createHARulesExecutorWithSessionState(rulesSet, rulesetString);
        } else {
            // normal non-HA mode
            executor = RulesExecutorFactory.createRulesExecutor(rulesSet);
        }

        rulesExecutorContainer.register( executor );
        configureScheduledMatchCallback(executor);

        return executor.getId();
    }

    public String sessionStats(long sessionId) {
        RulesExecutor rulesExecutor = rulesExecutorContainer.get(sessionId);
        return rulesExecutor == null ? null : toJson( rulesExecutor.getSessionStats() );
    }

    public String dispose(long sessionId) {
        RulesExecutor rulesExecutor = rulesExecutorContainer.get(sessionId);
        if (rulesExecutor == null) {
            return null;
        }
        if (haMode && haStateManager != null) {
            haStateManager.unregisterSessionState(rulesExecutor.getRuleSetName());
        }
        return toJson( rulesExecutor.dispose() );
    }

    @Deprecated
    public String retractFact(long sessionId, String serializedFact) {
        return matchesToJson( rulesExecutorContainer.get(sessionId).processRetractMatchingFacts(serializedFact, false).join() );
    }

    public String retractMatchingFacts(long sessionId, String serializedFact, boolean allowPartialMatch, String... keysToExclude) {
        List<Match> matches = rulesExecutorContainer.get(sessionId).processRetractMatchingFacts(serializedFact, allowPartialMatch, keysToExclude).join();

        // HA mode: persist state changes from retraction
        // Note: Retractions can trigger rule matches (e.g., IsNotDefinedExpression, negation patterns)
        // so we reuse processFactOrEventHA to handle both state persistence and potential MatchingEvents.
        // Side effect: increments eventsProcessedInTerm counter even though this is a retraction operation.
        if (haMode && haStateManager != null) {
            return processFactOrEventHA(sessionId, matches);
        }

        return matchesToJson(matches);
    }

    public String assertFact(long sessionId, String serializedFact) {
        logger.debug("received fact {}", serializedFact);
        List<Match> matches = rulesExecutorContainer.get(sessionId).processFacts(serializedFact).join();

        if (haMode && haStateManager != null) {
            return processFactOrEventHA(sessionId, matches);
        }

        return matchesToJson(matches);
    }

    public String assertEvent(long sessionId, String serializedFact) {
        logger.debug("received event {}", serializedFact);
        RulesExecutor executor = rulesExecutorContainer.get(sessionId);
        List<Match> matches = executor.processEvents(serializedFact).join();

        if (haMode && haStateManager != null) {
            return processFactOrEventHA(sessionId, matches);
        }

        return matchesToJson(matches);
    }

    /**
     * Common method to handle both event and fact processing in HA mode.
     * Updates in-memory SessionState for both leader and non-leader nodes.
     * Leader nodes also persist to database atomically with matching events.
     */
    private String processFactOrEventHA(long sessionId, List<Match> matches) {
        List<Map<String, Map<String, Object>>> matchList = RuleMatch.asList(matches);
        return toJson(processMatchesHA(sessionId, matchList));
    }

    /**
     * Core HA pipeline: update in-memory state, build matching events, and persist atomically.
     * Shared by processFactOrEventHA (synchronous path) and handleScheduledMatchesHA (auto-clock path).
     *
     * @return the HA response list
     */
    private List<Map<String, Object>> processMatchesHA(long sessionId,
                                                        List<Map<String, Map<String, Object>>> matchList) {
        HARulesExecutor rulesExecutor = (HARulesExecutor) rulesExecutorContainer.get(sessionId);
        String rulesetName = rulesExecutor.getRulesSet().getName();

        SessionState sessionState = haStateManager.getInMemorySessionState(rulesetName);
        if (sessionState == null) {
            throw new IllegalStateException("No in-memory SessionState found for " + rulesetName + ". This should never happen — registerSessionState must be called during createRuleset.");
        }

        updateInMemorySessionState(rulesExecutor, sessionState);

        boolean isLeader = haStateManager.isLeader();

        // Phase 1: Build matching events in-memory (assign UUIDs, populate fields)
        List<MatchingEvent> matchingEvents = new ArrayList<>();
        List<Map<String, Object>> haMatches = buildMatchingEventsAndResponse(sessionId, matchList, isLeader, matchingEvents);

        // Phase 2: Persist state + stats + matching events atomically (leader only)
        if (isLeader) {
            HAStats haStats = haStateManager.getHAStats();
            haStats.incrementEventsProcessed();
            updateGlobalSessionStats(haStats);
            haStateManager.persistSessionStateStatsAndMatchingEvents(sessionState, matchingEvents);
        }

        return haMatches;
    }

    /**
     * Update in-memory SessionState with current session data.
     * This method is called for both leader and non-leader nodes after processing an event/fact.
     */
    private void updateInMemorySessionState(HARulesExecutor rulesExecutor, SessionState sessionState) {
        HASessionContext haSessionContext = rulesExecutor.getHaSessionContext();
        LinkedHashMap<String, EventRecord> recordsInMemory = haSessionContext.getTrackedRecords();

        // Update partial events from memory
        sessionState.setPartialEvents(new ArrayList<>(recordsInMemory.values()));

        // Update processed event IDs from memory
        sessionState.setProcessedEventIds(haSessionContext.getProcessedEventIds());

        // Update persisted time
        sessionState.setPersistedTime(rulesExecutor.asKieSession().getSessionClock().getCurrentTime());

        sessionState.setLeaderId(haStateManager.getLeaderId());

        // Calculate integrity SHA from complete state
        updateStateSHA(sessionState);
    }

    /**
     * Update SHA for integrity verification.
     * Calculate SHA from complete state content to detect corruption/tampering.
     */
    private void updateStateSHA(SessionState sessionState) {
        if (sessionState == null) {
            return;
        }

        // Calculate SHA from complete state content
        String newSHA = calculateStateSHA(sessionState);
        sessionState.setCurrentStateSHA(newSHA);
    }

    /**
     * Build MatchingEvent objects in-memory and construct the HA response list.
     * No database I/O happens in this method — persistence is done separately.
     *
     * @param sessionId the session ID
     * @param matchList the rule matches
     * @param isLeader whether this node is the leader (determines UUID assignment)
     * @param matchingEventsOut output list populated with MatchingEvent objects for leader persistence
     * @return the HA response list with meUuids populated
     */
    private List<Map<String, Object>> buildMatchingEventsAndResponse(long sessionId,
                                                                      List<Map<String, Map<String, Object>>> matchList,
                                                                      boolean isLeader,
                                                                      List<MatchingEvent> matchingEventsOut) {
        if (matchList.isEmpty()) {
            return new ArrayList<>();
        }

        String rulesetName = rulesExecutorContainer.get(sessionId).getRuleSetName();
        List<Map<String, Object>> haMatches = new ArrayList<>(matchList.size());
        for (Map<String, Map<String, Object>> matchData : matchList) {
            String ruleName = matchData.keySet().iterator().next();
            Map<String, Object> eventData = matchData.get(ruleName);

            String meUuid;
            if (isLeader) {
                MatchingEvent me = new MatchingEvent();
                me.setHaUuid(haStateManager.getHaUuid());
                me.setRuleSetName(rulesetName);
                me.setRuleName(ruleName);
                me.setEventData(toJson(eventData));

                // Assign UUID in-memory (will be persisted later in the combined transaction)
                meUuid = UUID.randomUUID().toString();
                me.setMeUuid(meUuid);
                me.setCreatedAt(System.currentTimeMillis());

                matchingEventsOut.add(me);
            } else {
                meUuid = EMPTY_ME_UUID; // Non-leader nodes do not execute actions with ME UUIDs
            }

            Map<String, Object> resultEntry = new LinkedHashMap<>();
            populateHAMatchResponse(resultEntry, ruleName, eventData, meUuid);
            haMatches.add(resultEntry);
        }
        return haMatches;
    }

    /**
     * Handle matches triggered by AutomaticPseudoClock through the HA pipeline.
     * This is called on the asyncExecutor thread via the callback set on HARulesEvaluator.
     */
    private List<Map<String, Object>> handleScheduledMatchesHA(long sessionId, List<Match> matches) {
        List<Map<String, Map<String, Object>>> matchList = RuleMatch.asList(matches);
        if (matchList.isEmpty()) {
            return null;
        }
        return processMatchesHA(sessionId, matchList);
    }

    /**
     * Configure the scheduled match callback on a HARulesExecutor so that
     * auto-clock matches are routed through the HA pipeline.
     */
    private void configureScheduledMatchCallback(RulesExecutor executor) {
        if (haMode && haStateManager != null && executor instanceof HARulesExecutor haExecutor) {
            long sessionId = haExecutor.getId();
            haExecutor.setScheduledMatchCallback(m -> handleScheduledMatchesHA(sessionId, m));
        }
    }

    /**
     * Advances the clock time in the specified unit amount.
     *
     * @param amount the amount of units to advance in the clock
     * @param unit the used time unit
     * @return the events that fired
     */
    public String advanceTime(long sessionId, long amount, String unit) {
        RulesExecutor executor = rulesExecutorContainer.get(sessionId);
        List<Match> matches = executor.advanceTime(amount, TimeUnit.valueOf(unit.toUpperCase())).join();
        if (haMode && haStateManager != null) {
            // In HA mode, HARulesEvaluator.advanceTime() routes through onScheduledMatches
            // which handles HA state persistence, matching event creation, and enriched channel write.
            // Use the already-built HA result for the sync return to avoid duplicate processing.
            if (executor instanceof HARulesExecutor haExecutor) {
                List<Map<String, Object>> haResult = haExecutor.consumeLastAdvanceTimeHAResult();
                if (haResult != null) {
                    return toJson(haResult);
                }
            }
            return processFactOrEventHA(sessionId, matches);
        }
        return matchesToJson(matches);
    }

    private static String matchesToJson(List<Match> matches) {
        return toJson(RuleMatch.asList(matches));
    }

    public String getFacts(long sessionId) {
        RulesExecutor executor = rulesExecutorContainer.get(sessionId);
        if (executor == null) {
            throw new NoSuchElementException("No such session id: " + sessionId + ". " + "Was it disposed?");
        }
        return toJson(executor.getAllFactsAsMap().stream().map(RulesModelUtil::factToMap).collect(Collectors.toList()));
    }

    public void shutdown() {
        close();
    }

    // This method is not exposed to drools_jpy. Rather shutdown is called to full dispose, but not delete HA SessionState records.
    @Override
    public void close() {
        shutdown = true;
        if (haStateManager != null) {
            haStateManager.shutdown();
        }
        rulesExecutorContainer.disposeAll();
    }

    public int port() {
        return rulesExecutorContainer.port();
    }

    private boolean rulebookHashMismatch(String rulesetName, String localHash, SessionState persistedState) {
        String persistedHash = persistedState.getRulebookHash();
        if (persistedHash == null || persistedHash.isEmpty()) {
            throw new IllegalStateException("Persisted SessionState is missing rulebookHash for " + rulesetName);
        }
        if (localHash == null || localHash.isEmpty()) {
            throw new IllegalStateException("Local rulebookHash is missing for " + rulesetName);
        }
        if (!persistedHash.equals(localHash)) {
            if (overwriteIfRulebookChanges) {
                logger.warn("Rulebook hash mismatch detected for {} (local {}, persisted {}); overwrite_if_rulebook_changes is true - deleting old state and starting fresh",
                        rulesetName, localHash, persistedHash);
                return true;
            }
            logger.info("Rulebook hash mismatch detected for {} (local {}, persisted {}), but overwrite_if_rulebook_changes is false - recovering from persisted state",
                    rulesetName, localHash, persistedHash);
            return false;
        }
        return false;
    }

    private void checkAlive() {
        if (shutdown) {
            throw new IllegalStateException("This AstRulesEngine is shutting down");
        }
    }
    
    // ========== High Availability APIs ==========
    
    /**
     * Initialize HA mode with UUID and database configuration
     * Called by Python: self._api.initializeHA(uuid, worker_name, db_params_json, config_json)
     */
    public void initializeHA(String uuid, String workerName, String dbParamsJson, String configJson) {
        logger.info("Initializing HA mode with UUID: {} and workerName: {}", uuid, workerName);

        try {
            Map<String, Object> dbParams = null;
            Map<String, Object> config = null;

            if (dbParamsJson != null && !dbParamsJson.isEmpty()) {
                dbParams = readValueAsMapOfStringAndObject(dbParamsJson);
            }

            if (configJson != null && !configJson.isEmpty()) {
                config = readValueAsMapOfStringAndObject(configJson);
            }

            // Extract db_type from dbParams (default: "postgres")
            String dbType = dbParams != null ? (String) dbParams.getOrDefault("db_type", "postgres") : "postgres";

            this.haStateManager = HAStateManagerFactory.create(dbType);
            this.haStateManager.initializeHA(uuid, workerName, dbParams, config);
            this.haMode = true;

            // Extract dedup buffer size from config
            this.dedupBufferSize = config != null
                    ? ((Number) config.getOrDefault("dedup_buffer_size", 5)).intValue()
                    : 5;
            logger.info("HA deduplication buffer size set to {}", dedupBufferSize);

            // Extract overwrite_if_rulebook_changes from config (default: true)
            if (config != null && config.containsKey("overwrite_if_rulebook_changes")) {
                this.overwriteIfRulebookChanges = Boolean.TRUE.equals(config.get("overwrite_if_rulebook_changes"));
            }
            logger.info("HA overwrite_if_rulebook_changes set to {}", overwriteIfRulebookChanges);

            // HA mode always requires async channel
            rulesExecutorContainer.allowAsync();

            logger.info("HA mode initialized successfully");
        } catch (Exception e) {
            logger.error("Failed to initialize HA mode", e);
            throw new RuntimeException("Failed to initialize HA mode: " + e.getMessage(), e);
        }
    }

    /**
     * Enable leader mode and start writing states to database
     * Called by Python: self._api.enableLeader()
     */
    public void enableLeader() {
        requireHaMode();

        // Without executors/sessions, we cannot send matching events to Python client
        requireRuleCreation();

        logger.info("Enabling leader mode for: {}", haStateManager.getWorkerName());

        // Phase 1: Read from DB + in-memory mutations (no DB writes)
        haStateManager.enableLeader();

        // Phase 2: Recover all sessions in-memory, collect persistence intents
        List<SessionState> statesToPersist = new ArrayList<>();
        List<String> rulesetsToDelete = new ArrayList<>();
        List<MatchingEvent> allRecoveryMatches = new ArrayList<>();

        for (RulesExecutor executor : rulesExecutorContainer.getAllExecutors()) {
            SessionRecoveryResult result = restoreSessionInMemory(executor);
            if (result != null) {
                if (result.deleteRulesetName() != null) {
                    rulesetsToDelete.add(result.deleteRulesetName());
                }
                if (result.sessionState() != null) {
                    statesToPersist.add(result.sessionState());
                }
                allRecoveryMatches.addAll(result.recoveryMatches());
            }
        }

        // Phase 3: ONE persist call — single transaction
        haStateManager.persistLeaderStartup(statesToPersist, rulesetsToDelete, allRecoveryMatches);
        for (MatchingEvent me : allRecoveryMatches) {
            logger.info("Persisted grace-period recovery match for rule '{}' as MatchingEvent {}", me.getRuleName(), me.getMeUuid());
        }

        // Phase 4: Post-persist (read-only, in-memory)
        updateGlobalSessionStats(haStateManager.getHAStats());
        recoverPendingMatchingEvents();
        haStateManager.logStartupSummary();
    }

    /**
     * Represents the result of one session's in-memory recovery (no DB writes yet).
     */
    private record SessionRecoveryResult(
        SessionState sessionState,
        List<MatchingEvent> recoveryMatches,
        String deleteRulesetName
    ) {}

    /**
     * Perform in-memory recovery for a single session and return persistence intents.
     * Does NOT write to DB.
     */
    private SessionRecoveryResult restoreSessionInMemory(RulesExecutor executor) {
        if (!(executor instanceof HARulesExecutor)) {
            throw new IllegalStateException("Expected HARulesExecutor in HA mode");
        }
        if (!haStateManager.isLeader()) {
            throw new IllegalStateException("This method should only be called by the leader");
        }

        String rulesetName = executor.getRuleSetName();

        // Get persisted state (from database)
        SessionState persistedSessionState = haStateManager.getPersistedSessionState(rulesetName);

        if (persistedSessionState == null) {
            // No persisted state - this is the first time for this ruleset
            return null;
        }

        // Verify integrity
        haStateManager.verifySessionState(persistedSessionState);

        // Check if ruleset has been updated
        String localHash = sha256(((HARulesExecutor) executor).getRulesetString());
        if (rulebookHashMismatch(rulesetName, localHash, persistedSessionState)) {
            logger.info("Ruleset updated for {} - will delete old session state and persist fresh state as leader", rulesetName);
            SessionState freshState = haStateManager.getInMemorySessionState(rulesetName);
            // Return delete intent + optional fresh state to persist
            return new SessionRecoveryResult(freshState, List.of(), rulesetName);
        }

        RulesExecutor recoveredRulesExecutor = haStateManager.recoverSession(((HARulesExecutor) executor).getRulesetString(), persistedSessionState, executor.asKieSession().getSessionClock().getCurrentTime());

        // Build grace-period recovery matches (not yet persisted)
        List<MatchingEvent> recoveryMatches = buildRecoveryMatchingEvents(recoveredRulesExecutor);

        long previousId = executor.getId();
        RulesExecutor removed = rulesExecutorContainer.removeExecutor(previousId);
        if (removed != null) {
            removed.dispose();
        }

        // Keep the same session ID for python client
        ((HARulesExecutor) recoveredRulesExecutor).setExternalSessionId(previousId);

        rulesExecutorContainer.register(recoveredRulesExecutor);
        configureScheduledMatchCallback(recoveredRulesExecutor);

        // Update in-memory state from recovered executor (refreshes partialEvents from WM)
        haStateManager.registerSessionState(rulesetName, persistedSessionState);
        updateInMemorySessionState((HARulesExecutor) recoveredRulesExecutor, persistedSessionState);

        logger.info("Recovered session {} from persisted SessionState", rulesetName);

        // Return persistence intents — no DB writes yet
        return new SessionRecoveryResult(persistedSessionState, recoveryMatches, null);
    }

    private RulesExecutor createHARulesExecutorWithSessionState(RulesSet rulesSet, String rulesetString) {
        if (rulesetString == null) {
            throw new IllegalStateException("null rulesetString is not allowed in HA mode");
        }

        String rulesetName = rulesSet.getName();
        String rulebookHash = sha256(rulesetString);

        // Check for persisted state from database. Leader only
        if (haStateManager.isLeader()) {
            SessionState persistedSessionState = haStateManager.getPersistedSessionState(rulesetName);

            if (persistedSessionState != null) {
                if (rulebookHashMismatch(rulesetName, rulebookHash, persistedSessionState)) {
                    logger.info("Ruleset updated for {} - deleting old session state and creating fresh session", rulesetName);
                    // deleteSessionState only; fresh state will be persisted below after creating new executor
                    haStateManager.deleteSessionState(rulesetName);
                    // Fall through to create fresh executor below
                } else {
                    // Persisted state exists with same rulebook - recover from it
                    RulesExecutor recoveredExecutor = haStateManager.recoverSession(rulesetString, persistedSessionState, System.currentTimeMillis());

                    // Build grace-period recovery matches (not yet persisted)
                    List<MatchingEvent> recoveryMatches = buildRecoveryMatchingEvents(recoveredExecutor);

                    // Set dedup buffer size on recovered executor
                    ((HARulesExecutor) recoveredExecutor).getHaSessionContext().setMaxProcessedIds(dedupBufferSize);

                    // Update in-memory state from recovered executor (refreshes partialEvents from WM)
                    haStateManager.registerSessionState(rulesetName, persistedSessionState);
                    updateInMemorySessionState((HARulesExecutor) recoveredExecutor, persistedSessionState);

                    // Persist refreshed state + recovery matches in single transaction
                    haStateManager.persistSessionStateAndMatchingEvents(persistedSessionState, recoveryMatches);
                    for (MatchingEvent me : recoveryMatches) {
                        logger.info("Persisted grace-period recovery match for rule '{}' as MatchingEvent {}", me.getRuleName(), me.getMeUuid());
                    }

                    return recoveredExecutor;
                }
            }
        }

        // No persisted state or non-leader - create fresh executor and initial SessionState
        RulesExecutor executor = HARulesExecutorFactory.createRulesExecutor(rulesSet, rulesetString);

        // Set dedup buffer size on the new executor
        ((HARulesExecutor) executor).getHaSessionContext().setMaxProcessedIds(dedupBufferSize);

        // Create initial SessionState (same for both leader and non-leader)
        SessionState sessionState = new SessionState();
        sessionState.setHaUuid(haStateManager.getHaUuid());
        sessionState.setRuleSetName(rulesetName);
        sessionState.setPartialEvents(new ArrayList<>());
        sessionState.setProcessedEventIds(new ArrayList<>());
        long currentTime = executor.asKieSession().getSessionClock().getCurrentTime();
        sessionState.setCreatedTime(currentTime);
        sessionState.setPersistedTime(currentTime);
        sessionState.setRulebookHash(rulebookHash);
        sessionState.setLeaderId(haStateManager.getLeaderId());

        // Calculate initial SHA from complete state
        sessionState.setCurrentStateSHA(calculateStateSHA(sessionState));

        // Register in memory (for both leader and non-leader)
        haStateManager.registerSessionState(rulesetName, sessionState);

        // Leader also persists to database
        if (haStateManager.isLeader()) {
            haStateManager.persistSessionState(sessionState);
        }

        return executor;
    }

    /**
     * Disable leader mode and stop writing to database
     * Called by Python: self._api.disableLeader()
     */
    public void disableLeader() {
        requireHaMode();

        logger.info("Disabling leader mode for: {}", haStateManager.getWorkerName());
        haStateManager.disableLeader();
    }
    
    /**
     * Add an action for a matching event
     * Called by Python: self._api.addActionInfo(session, matching_uuid, index, action)
     */
    public void addActionInfo(long sessionId, String matchingUuid, int index, String action) {
        requireLeader();
        haStateManager.addActionInfo(matchingUuid, index, action);
        logger.debug("Added action at index {} for ME UUID: {}", index, matchingUuid);
    }
    
    /**
     * Update an existing action
     * Called by Python: self._api.updateActionInfo(session, matching_uuid, index, action)
     */
    public void updateActionInfo(long sessionId, String matchingUuid, int index, String action) {
        requireLeader();
        haStateManager.updateActionInfo(matchingUuid, index, action);
        logger.debug("Updated action at index {} for ME UUID: {}", index, matchingUuid);
    }
    
    /**
     * Check if an action exists
     * Called by Python: self._api.actionInfoExists(session, matching_uuid, index)
     */
    public boolean actionInfoExists(long sessionId, String matchingUuid, int index) {
        requireLeader();
        return haStateManager.actionInfoExists(matchingUuid, index);
    }
    
    /**
     * Get an action by index
     * Called by Python: self._api.getActionInfo(session, matching_uuid, index)
     */
    public String getActionInfo(long sessionId, String matchingUuid, int index) {
        requireLeader();
        return haStateManager.getActionInfo(matchingUuid, index);
    }

    /**
     * Get the stored status for an action
     * Called by Python: self._api.getActionStatus(session, matching_uuid, index)
     */
    public String getActionStatus(long sessionId, String matchingUuid, int index) {
        requireLeader();
        return haStateManager.getActionStatus(matchingUuid, index);
    }

    /**
     * Delete all actions and matching events for a matching UUID
     * Called by Python: self._api.deleteActionInfo(session, matching_uuid)
     */
    public void deleteActionInfo(long sessionId, String matchingUuid) {
        requireLeader();
        haStateManager.deleteActionInfo(matchingUuid);
        logger.debug("Deleted all actions for ME UUID: {}", matchingUuid);
    }

    private void requireHaMode() {
        if (!haMode || haStateManager == null) {
            throw new IllegalStateException("HA mode not initialized");
        }
    }

    private void requireLeader() {
        requireHaMode();
        if (!haStateManager.isLeader()) {
            throw new IllegalStateException("This operation can only be performed by the leader");
        }
    }

    private void requireRuleCreation() {
        if (rulesExecutorContainer.isEmpty()) {
            throw new IllegalStateException("No rulesets created yet. Please create a ruleset before performing this operation.");
        }
    }
    
    /**
     * Get current HA statistics
     * Called by Python: self._api.getHAStats()
     */
    public String getHAStats() {
        requireHaMode();

        haStateManager.refreshHAStats();
        HAStats stats = haStateManager.getHAStats();
        stats.setPartialFulfilledRules(computePartialFulfilledRules());
        Map<String, Object> result = new HashMap<>();
        result.put("ha_uuid", stats.getHaUuid());
        result.put("current_leader", stats.getCurrentLeader());
        result.put("leader_switches", stats.getLeaderSwitches());
        result.put("current_term_started_at", stats.getCurrentTermStartedAt());
        result.put("events_processed_in_term", stats.getEventsProcessedInTerm());
        result.put("actions_processed_in_term", stats.getActionsProcessedInTerm());
        result.put("incomplete_matching_events", stats.getIncompleteMatchingEvents());
        result.put("partial_events_in_memory", stats.getPartialEventsInMemory());
        result.put("partial_fulfilled_rules", stats.getPartialFulfilledRules());
        updateGlobalSessionStats(stats);
        result.put("global_session_stats", stats.getGlobalSessionStats());
        result.put("session_state_size", stats.getSessionStateSize());

        return toJson(result);
    }

    /**
     * Get IDs of events currently held in working memory awaiting rule completion.
     * Only returns EVENT type records (excludes FACT and CONTROL_* types).
     *
     * Called by Python: self._api.getPartialEventIds(session_id)
     *
     * @param sessionId The session ID
     * @return JSON array of event ID strings
     */
    public String getPartialEventIds(long sessionId) {
        requireHaMode();

        RulesExecutor executor = rulesExecutorContainer.get(sessionId);
        if (executor == null) {
            throw new NoSuchElementException("No such session id: " + sessionId + ". Was it disposed?");
        }

        if (!(executor instanceof HARulesExecutor haExecutor)) {
            return "[]";
        }

        HASessionContext ctx = haExecutor.getHaSessionContext();
        Map<String, EventRecord> records = ctx.getTrackedRecords();

        List<String> ids = records.entrySet().stream()
                .filter(e -> e.getValue().getRecordType() == EventRecord.RecordType.EVENT)
                .map(Map.Entry::getKey)
                .toList();

        return toJson(ids);
    }

    private int computePartialFulfilledRules() {
        if (rulesExecutorContainer == null) {
            return 0;
        }

        int total = 0;
        for (RulesExecutor executor : rulesExecutorContainer.getAllExecutors()) {
            try {
                total += PartialMatchCounter.countPartialTuplesTotal(executor.asKieSession());
            } catch (Exception e) {
                logger.debug("Failed to count partial matches for executor {}", executor.getId(), e);
            }
        }
        return total;
    }

    private SessionStats aggregateAllSessionStats() {
        Collection<RulesExecutor> executors = rulesExecutorContainer.getAllExecutors();
        SessionStats aggregate = null;
        for (RulesExecutor executor : executors) {
            SessionStats stats = executor.getSessionStats();
            if (stats == null) {
                continue;
            }
            aggregate = aggregate == null ? stats : SessionStats.aggregate(aggregate, stats);
        }
        return aggregate;
    }

    private void updateGlobalSessionStats(HAStats haStats) {
        if (haStats == null || !haMode || haStateManager == null || !haStateManager.isLeader()) {
            return;
        }

        SessionStats currentAggregate = aggregateAllSessionStats();
        if (currentAggregate == null) {
            return;
        }

        SessionStats lastSnapshot = lastAggregatedSessionStatsByLeader.get(haStateManager.getLeaderId());
        int deltaRulesTriggered = currentAggregate.getRulesTriggered() - (lastSnapshot == null ? 0 : lastSnapshot.getRulesTriggered());
        int deltaEventsProcessed = currentAggregate.getEventsProcessed() - (lastSnapshot == null ? 0 : lastSnapshot.getEventsProcessed());
        int deltaEventsMatched = currentAggregate.getEventsMatched() - (lastSnapshot == null ? 0 : lastSnapshot.getEventsMatched());
        int deltaEventsSuppressed = currentAggregate.getEventsSuppressed() - (lastSnapshot == null ? 0 : lastSnapshot.getEventsSuppressed());
        int deltaClockAdvances = currentAggregate.getClockAdvanceCount() - (lastSnapshot == null ? 0 : lastSnapshot.getClockAdvanceCount());
        int deltaAsyncResponses = currentAggregate.getAsyncResponses() - (lastSnapshot == null ? 0 : lastSnapshot.getAsyncResponses());
        int deltaBytesSent = currentAggregate.getBytesSentOnAsync() - (lastSnapshot == null ? 0 : lastSnapshot.getBytesSentOnAsync());

        SessionStats existingGlobal = haStats.getGlobalSessionStats();
        String start = existingGlobal == null ? currentAggregate.getStart() : existingGlobal.getStart();

        SessionStats merged = new SessionStats(
                start,
                currentAggregate.getEnd(),
                currentAggregate.getLastClockTime(),
                (existingGlobal == null ? 0 : existingGlobal.getClockAdvanceCount()) + deltaClockAdvances,
                currentAggregate.getNumberOfRules(),
                currentAggregate.getNumberOfDisabledRules(),
                (existingGlobal == null ? 0 : existingGlobal.getRulesTriggered()) + deltaRulesTriggered,
                (existingGlobal == null ? 0 : existingGlobal.getEventsProcessed()) + deltaEventsProcessed,
                (existingGlobal == null ? 0 : existingGlobal.getEventsMatched()) + deltaEventsMatched,
                (existingGlobal == null ? 0 : existingGlobal.getEventsSuppressed()) + deltaEventsSuppressed,
                currentAggregate.getPermanentStorageCount(),
                currentAggregate.getPermanentStorageSize(),
                (existingGlobal == null ? 0 : existingGlobal.getAsyncResponses()) + deltaAsyncResponses,
                (existingGlobal == null ? 0 : existingGlobal.getBytesSentOnAsync()) + deltaBytesSent,
                currentAggregate.getSessionId(),
                currentAggregate.getRuleSetName(),
                currentAggregate.getLastRuleFired(),
                currentAggregate.getLastRuleFiredAt(),
                currentAggregate.getLastEventReceivedAt(),
                Math.max(existingGlobal == null ? 0 : existingGlobal.getBaseLevelMemory(), currentAggregate.getBaseLevelMemory()),
                Math.max(existingGlobal == null ? 0 : existingGlobal.getPeakMemory(), currentAggregate.getPeakMemory())
        );

        haStats.setGlobalSessionStats(merged);
        lastAggregatedSessionStatsByLeader.put(haStateManager.getLeaderId(), currentAggregate);
    }

    /**
     * Build MatchingEvent objects from grace-period-eligible recovery matches.
     * Returns empty list if no recovery matches exist.
     */
    private List<MatchingEvent> buildRecoveryMatchingEvents(RulesExecutor executor) {
        if (!(executor instanceof HARulesExecutor haExecutor)) return List.of();

        List<Match> recoveryMatches = haExecutor.consumeRecoveryMatches();
        if (recoveryMatches == null || recoveryMatches.isEmpty()) return List.of();

        String rulesetName = executor.getRuleSetName();
        List<Map<String, Map<String, Object>>> matchList = RuleMatch.asList(recoveryMatches);

        List<MatchingEvent> matchingEvents = new ArrayList<>();
        for (Map<String, Map<String, Object>> matchData : matchList) {
            String ruleName = matchData.keySet().iterator().next();
            Map<String, Object> eventData = matchData.get(ruleName);

            MatchingEvent me = new MatchingEvent();
            me.setHaUuid(haStateManager.getHaUuid());
            me.setRuleSetName(rulesetName);
            me.setRuleName(ruleName);
            me.setEventData(toJson(eventData));
            matchingEvents.add(me);
        }

        return matchingEvents;
    }

    /**
     * Recover pending matching events when becoming leader
     */
    private void recoverPendingMatchingEvents() {
        // Async channel must be available at this point
        if (rulesExecutorContainer.getChannel() == null) {
            throw new RuntimeException("Async channel is null. There should be a problem in API calling sequence.");
        }
        if (!rulesExecutorContainer.getChannel().isConnected()) {
            // It may be a python coroutine issue where asyncio.open_connection is blocked in event loop
            throw new RuntimeException("Async channel connection is not open yet. The Python client may not connect yet.");
        }

        logger.info("Checking for pending matching events to recover");

        List<MatchingEvent> allPendingEvents = haStateManager.getPendingMatchingEvents();
        if (allPendingEvents.isEmpty()) {
            logger.info("No pending matching events to recover");
            return;
        }

        // Group MEs by ruleSetName so each executor only gets its own MEs
        Map<String, List<MatchingEvent>> eventsByRuleSetName = allPendingEvents.stream()
                .collect(Collectors.groupingBy(MatchingEvent::getRuleSetName));

        for (RulesExecutor executor : rulesExecutorContainer.getAllExecutors()) {
            List<MatchingEvent> pendingEvents = eventsByRuleSetName.remove(executor.getRuleSetName());
            if (pendingEvents != null && !pendingEvents.isEmpty()) {
                logger.info("Found {} pending matching events for session {} : {}",
                            pendingEvents.size(), executor.getId(), executor.getRuleSetName());
                sendMatchingEventRecovery(executor.getId(), pendingEvents);
            }
        }

        // Warn about orphaned MEs whose ruleSetName doesn't match any current executor
        if (!eventsByRuleSetName.isEmpty()) {
            eventsByRuleSetName.forEach((ruleSetName, events) ->
                    logger.warn("Found {} orphaned pending matching events for unknown ruleset '{}': {}",
                                events.size(), ruleSetName, events.stream().map(MatchingEvent::getMeUuid).toList()));
        }
    }
    
    /**
     * Send a matching event recovery notification through the async channel
     */
    private void sendMatchingEventRecovery(long sessionId, List<MatchingEvent> matchingEvents) {
        if (rulesExecutorContainer.getChannel() == null || !rulesExecutorContainer.getChannel().isConnected()) {
            logger.warn("Async channel not available for ME recovery: {}", matchingEvents.stream().map(MatchingEvent::getMeUuid).toList());
            return;
        }

        List<Map<String, Object>> resultList = new ArrayList<>();

        for (MatchingEvent matchingEvent : matchingEvents) {
            // Parse event data JSON back to object for compatibility
            Map<String, Object> eventData = new HashMap<>();
            try {
                if (matchingEvent.getEventData() != null) {
                    eventData = readValueAsMapOfStringAndObject(matchingEvent.getEventData());
                }
            } catch (Exception e) {
                logger.warn("Failed to parse event data JSON for recovery", e);
                // TBD: Should throw RuntimeException here?
            }

            // Create recovery payload with ME UUID
            Map<String, Object> result = new HashMap<>();
            populateHAMatchResponse(result,
                                    matchingEvent.getRuleName(),
                                    eventData,
                                    matchingEvent.getMeUuid()); // these 3 fields are conformed to HA match response format
            result.put("type", "MATCHING_EVENT_RECOVERY");
            result.put("ruleset_name", matchingEvent.getRuleSetName());
            result.put("created_at", matchingEvent.getCreatedAt());

            resultList.add(result);
        }

        
        // Send through async channel
        Response response = new Response(sessionId, resultList); // List is expected by Python side
        rulesExecutorContainer.getChannel().write(response);
        
        logger.info("Sent ME recovery notification for UUID: {} on session: {}",
                    matchingEvents.stream().map(MatchingEvent::getMeUuid).toList(), sessionId);
    }
}
