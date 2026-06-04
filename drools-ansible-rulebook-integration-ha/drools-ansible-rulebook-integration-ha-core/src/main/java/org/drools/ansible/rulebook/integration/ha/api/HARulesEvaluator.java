package org.drools.ansible.rulebook.integration.ha.api;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.drools.ansible.rulebook.integration.api.domain.RuleMatch;
import org.drools.ansible.rulebook.integration.api.domain.RulesSet;
import org.drools.ansible.rulebook.integration.api.io.Response;
import org.drools.ansible.rulebook.integration.api.rulesengine.RulesExecutorSession;
import org.drools.ansible.rulebook.integration.api.rulesengine.SyncRulesEvaluator;
import org.drools.core.common.InternalFactHandle;
import org.kie.api.runtime.rule.Match;

import static org.drools.ansible.rulebook.integration.ha.api.HAUtils.getEventUuid;

/**
 * Extends SyncRulesEvaluator.
 * AsyncRulesEvaluator is only used by AsyncAstRulesEngine (which sets RuleConfigurationOption.ASYNC_EVALUATION), which uses async channel even for synchronous evaluation (processEvent).
 * But AsyncAstRulesEngine is not used by drools_jpy as of 1.0.11.
 *
 * So the HA scenario is HARulesEvaluator + async channel
 */
public class HARulesEvaluator extends SyncRulesEvaluator {

    // External session ID for HA mode - may differ from internal session ID after recovery
    private Long externalSessionId;

    private volatile boolean onRecovery = false;

    // Callback for handling auto-clock matches through HA pipeline
    private volatile Function<List<Match>, List<Map<String, Object>>> scheduledMatchCallback;

    // Stores the last HA-enriched result from advanceTime for sync return by AstRulesEngine
    private volatile List<Map<String, Object>> lastAdvanceTimeHAResult;

    public HARulesEvaluator(RulesExecutorSession rulesExecutorSession) {
        super(rulesExecutorSession);
    }

    /**
     * Override advanceTime to route through onScheduledMatches instead of writeResponseOnChannel.
     * This ensures the HA pipeline enriches the async channel response with matching_uuid.
     */
    @Override
    public CompletableFuture<List<Match>> advanceTime(long amount, TimeUnit unit) {
        lastAdvanceTimeHAResult = null;
        if (channel == null) {
            return engineEvaluate(() -> internalAdvanceTime(amount, unit));
        }
        return engineEvaluate(() -> onScheduledMatches(internalAdvanceTime(amount, unit)));
    }

    /**
     * Returns the HA-enriched result from the last advanceTime call, or null if not available.
     * Used by AstRulesEngine to build the sync return value without re-processing.
     */
    public List<Map<String, Object>> consumeLastAdvanceTimeHAResult() {
        List<Map<String, Object>> result = lastAdvanceTimeHAResult;
        lastAdvanceTimeHAResult = null;
        return result;
    }

    @Override
    protected List<Match> writeResponseOnChannel(List<Match> matches) {
        // Do not send responses if we are recovering the session
        if (!onRecovery && !matches.isEmpty()) { // skip empty result
            byte[] bytes = channel.write(new Response(getSessionId(), RuleMatch.asList(matches)));
            rulesExecutorSession.registerAsyncResponse(bytes);
        }
        return matches;
    }

    public void setOnRecovery(boolean onRecovery) {
        this.onRecovery = onRecovery;
    }

    public boolean isOnRecovery() {
        return onRecovery;
    }

    /**
     * Sets the external session ID for this evaluator.
     * This ID is used for container lookups and should match what Python client uses.
     *
     * @param externalSessionId The container lookup ID
     */
    public void setExternalSessionId(Long externalSessionId) {
        this.externalSessionId = externalSessionId;

        // Also set it on the session so SessionStats will use it
        if (rulesExecutorSession instanceof HARulesExecutorSession) {
            ((HARulesExecutorSession) rulesExecutorSession).setExternalSessionId(externalSessionId);
        }
    }

    @Override
    public long getSessionId() {
        return externalSessionId != null ? externalSessionId : super.getSessionId();
    }

    public RulesSet getRulesSet() {
        return ((HARulesExecutorSession) rulesExecutorSession).getRulesSet();
    }

    public HASessionContext getHaSessionContext() {
        return ((HARulesExecutorSession) rulesExecutorSession).getHaSessionContext();
    }

    public void setScheduledMatchCallback(Function<List<Match>, List<Map<String, Object>>> callback) {
        this.scheduledMatchCallback = callback;
    }

    @Override
    protected List<Match> onScheduledMatches(List<Match> matches) {
        if (onRecovery || matches.isEmpty()) {
            return matches;
        }
        Function<List<Match>, List<Map<String, Object>>> callback = this.scheduledMatchCallback;
        if (callback != null) {
            List<Map<String, Object>> haResult = callback.apply(matches);
            if (haResult != null) {
                lastAdvanceTimeHAResult = haResult;
                byte[] bytes = channel.write(new Response(getSessionId(), haResult));
                rulesExecutorSession.registerAsyncResponse(bytes);
                return matches;
            }
        }
        return super.onScheduledMatches(matches);
    }

    @Override
    protected void processDiscardedFact(InternalFactHandle fh) {
        getEventUuid(fh).ifPresentOrElse(
                uuid -> getHaSessionContext().discardTrackedRecord(uuid),
                () -> getHaSessionContext().discardTrackedRecordByFactHandle(fh.getId()));
    }
}
