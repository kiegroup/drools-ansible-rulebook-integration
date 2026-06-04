package org.drools.ansible.rulebook.integration.ha.api;

import java.util.List;
import java.util.Map;

import org.drools.ansible.rulebook.integration.api.RulesExecutor;
import org.drools.ansible.rulebook.integration.api.domain.RulesSet;
import org.drools.ansible.rulebook.integration.ha.model.SessionState;
import org.drools.ansible.rulebook.integration.ha.model.HAStats;
import org.drools.ansible.rulebook.integration.ha.model.EventRecordChange;
import org.drools.ansible.rulebook.integration.ha.model.EventRecordEntry;
import org.drools.ansible.rulebook.integration.ha.model.MatchingEvent;

/**
 * Interface for managing High Availability state persistence.
 * Implementations should handle connection management, transactions, and state consistency.
 */
public interface HAStateManager {

    /**
     * Initialize HA mode with UUID and database configuration
     *
     * @param uuid           Unique identifier for this HA instance
     * @param workerName     Name/identifier for this worker/node
     * @param dbParams Database connection parameters
     * @param config         HA configuration parameters
     */
    void initializeHA(String uuid, String workerName, Map<String, Object> dbParams, Map<String, Object> config);

    /**
     * Get the worker name/identifier
     *
     * @return The worker name
     */
    String getWorkerName();

    /**
     * Enable leader mode and start writing states to database
     *
     */
    void enableLeader();

    /**
     * Disable leader mode and stop writing to database
     *
     */
    void disableLeader();

    /**
     * Check if this node is currently the leader
     *
     * @return true if this node is the leader
     */
    boolean isLeader();

    /**
     * Get the UUID of this HA instance
     *
     * @return The HA instance UUID
     */
    String getHaUuid();

    /**
     * Get the leader ID
     *
     * @return The leader ID or null if no leader
     */
    String getLeaderId();

    /**
     * Get current session state for a session
     * This reads from the database (persisted state)
     *
     * @return The current session state or null if not found
     */
    SessionState getPersistedSessionState(String ruleSetName);

    /**
     * Persist complete session state (includes matching events)
     *
     * @param sessionState The session state to persist
     */
    void persistSessionState(SessionState sessionState);

    /**
     * Persist row-level retained EventRecord changes for a ruleset.
     *
     * @param ruleSetName The ruleset name
     * @param eventRecordChanges Row-level upsert/delete changes
     */
    void persistEventRecordChanges(String ruleSetName, List<EventRecordChange> eventRecordChanges);

    /**
     * Load retained EventRecord rows for a ruleset in recovery order.
     *
     * @param ruleSetName The ruleset name
     * @return ordered retained EventRecord entries
     */
    List<EventRecordEntry> getPersistedEventRecords(String ruleSetName);

    /**
     * Recover drools session using session state from the database
     * This should be called on startup to restore state
     *
     * @return
     */
    RulesExecutor recoverSession(String rulesetString, SessionState sessionState, long currentTimeAtNewNode);

    /**
     * Add a matching event to the database
     *
     * @param matchingEvent The matching event to persist
     * @return UUID for the matching event
     */
    String addMatchingEvent(MatchingEvent matchingEvent);

    /**
     * Add multiple matching events in a single transaction.
     *
     * @param matchingEvents The matching events to persist
     * @return List of UUIDs for the matching events
     */
    List<String> addMatchingEvents(List<MatchingEvent> matchingEvents);

    /**
     * Get all pending matching events based on haUuid
     *
     * @return List of pending matching events
     */
    List<MatchingEvent> getPendingMatchingEvents();

    /**
     * Add an action for a matching event
     *
     * @param matchingUuid The matching event UUID
     * @param index        The action index
     * @param action       The action data as JSON string
     */
    void addActionInfo(String matchingUuid, int index, String action);

    /**
     * Update an existing action
     *
     * @param matchingUuid The matching event UUID
     * @param index        The action index
     * @param action       The updated action data as JSON string
     */
    void updateActionInfo(String matchingUuid, int index, String action);

    /**
     * Check if an action exists
     *
     * @param matchingUuid The matching event UUID
     * @param index        The action index
     * @return true if action exists, false otherwise
     */
    boolean actionInfoExists(String matchingUuid, int index);

    /**
     * Get an action by index
     *
     * @param matchingUuid The matching event UUID
     * @param index        The action index
     * @return The action data as JSON string, empty string if not found
     */
    String getActionInfo(String matchingUuid, int index);

    String getActionStatus(String matchingUuid, int index);

    /**
     * Delete all actions and matching events for a matching UUID
     *
     * @param matchingUuid The matching event UUID
     */
    void deleteActionInfo(String matchingUuid);

    /**
     * Load existing HA statistics or create new if none exist
     *
     * @return HA statistics object
     */
    HAStats loadOrCreateHAStats();

    /**
     * Get current HA statistics (in-memory only, no DB queries).
     *
     * @return HA statistics object
     */
    HAStats getHAStats();

    /**
     * Refresh HAStats with values that require DB queries (e.g., incomplete matching events count).
     * This is expensive (DB round-trip) and should only be called when
     * fresh counts are needed for reporting (e.g., Python API getHAStats).
     */
    void refreshHAStats();

    /**
     * Persist current HA statistics
     */
    void persistHAStats();

    /**
     * Persist session state and HA stats in a single transaction.
     */
    void persistSessionStateAndStats(SessionState sessionState);

    /**
     * Persist session state, HA stats, and matching events in a single transaction.
     * This ensures atomicity: a crash cannot leave session state advanced while matching events are missing.
     *
     * @param sessionState the session state to persist
     * @param matchingEvents the matching events to insert (may be empty)
     */
    void persistSessionStateStatsAndMatchingEvents(SessionState sessionState, List<MatchingEvent> matchingEvents);

    /**
     * Persist session state, HA stats, row-level retained EventRecord changes,
     * and matching events in a single transaction.
     */
    void persistSessionStateStatsEventRecordsAndMatchingEvents(SessionState sessionState,
                                                               List<EventRecordChange> eventRecordChanges,
                                                               List<MatchingEvent> matchingEvents);

    /**
     * Persist session state, HA stats, a full retained EventRecord row snapshot,
     * and matching events in a single transaction.
     * Intended for recovery/startup flows where the retained set may shrink
     * without explicit delete deltas.
     */
    void persistSessionStateStatsEventRecordEntriesAndMatchingEvents(SessionState sessionState,
                                                                     List<EventRecordEntry> eventRecordEntries,
                                                                     List<MatchingEvent> matchingEvents);

    /**
     * Delete the persisted SessionState record for a given ruleset.
     *
     * @param ruleSetName The name of the ruleset
     */
    void deleteSessionState(String ruleSetName);

    /**
     * Persist session state and matching events in a single transaction.
     * Used after session recovery to atomically persist refreshed state and grace-period matches.
     *
     * @param sessionState   The session state to persist
     * @param matchingEvents The matching events to insert (may be empty)
     */
    void persistSessionStateAndMatchingEvents(SessionState sessionState, List<MatchingEvent> matchingEvents);

    /**
     * Persist session state, row-level retained EventRecord changes, and matching
     * events in a single transaction. Used after recovery.
     */
    void persistSessionStateEventRecordsAndMatchingEvents(SessionState sessionState,
                                                          List<EventRecordChange> eventRecordChanges,
                                                          List<MatchingEvent> matchingEvents);

    /**
     * Persist session state, a full retained EventRecord row snapshot, and
     * matching events in a single transaction.
     * Intended for recovery/startup flows where the retained set may shrink
     * without explicit delete deltas.
     */
    void persistSessionStateEventRecordEntriesAndMatchingEvents(SessionState sessionState,
                                                                List<EventRecordEntry> eventRecordEntries,
                                                                List<MatchingEvent> matchingEvents);

    /**
     * Persist all leader startup DB writes in a single transaction.
     * Called once at the end of enableLeader() after all in-memory recovery is complete.
     * <p>
     * One transaction that:
     * 1. Upserts HAStats (leader switch already mutated in memory)
     * 2. Deletes old session states (hash-mismatch rulesets)
     * 3. Upserts all refreshed session states
     * 4. Inserts all recovery matching events
     *
     * @param sessionStatesToPersist session states to upsert
     * @param rulesetNamesToDelete   ruleset names whose old session state should be deleted first
     * @param matchingEvents         recovery matching events to insert
     */
    void persistLeaderStartup(List<SessionState> sessionStatesToPersist,
                              Map<String, List<EventRecordEntry>> eventRecordEntriesByRuleSet,
                              List<String> rulesetNamesToDelete,
                              List<MatchingEvent> matchingEvents);

    /**
     * Cleanup resources and close connections
     */
    void shutdown();

    /**
     * Register session state in memory (for both leader and non-leader nodes)
     *
     * @param ruleSetName The name of the ruleset
     * @param sessionState The session state to register in memory
     */
    void registerSessionState(String ruleSetName, SessionState sessionState);

    /**
     * Get the in-memory session state for a given ruleset
     *
     * @param ruleSetName The name of the ruleset
     * @return The in-memory session state or null if not found
     */
    SessionState getInMemorySessionState(String ruleSetName);

    /**
     * Unregister the in-memory session state for a given ruleset (e.g. on dispose)
     *
     * @param ruleSetName The name of the ruleset
     */
    void unregisterSessionState(String ruleSetName);

    /**
     * Verify the integrity of a loaded SessionState.
     * Compares stored SHA with recalculated SHA to detect corruption/tampering.
     *
     * @param sessionState The state loaded from persistence
     * @return true if integrity check passes, false if corruption detected
     */
    boolean verifySessionState(SessionState sessionState);

    /**
     * Log a summary of pending items in the database at leader startup.
     */
    void logStartupSummary();

    /**
     * for debug
     */
    default void printDatabaseContents() {
        // Default no-op
    }
}
